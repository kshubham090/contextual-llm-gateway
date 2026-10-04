"""Tenant-scoped pgvector memory, billing records, and a transactional graph outbox."""

import base64
import hashlib
import json
import math
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import asyncpg

from .config import settings

MIGRATIONS = Path(__file__).resolve().parent.parent / "migrations"
MIGRATION_LOCK = 827413509
QUARANTINE_TENANT = "__legacy_quarantine__"


class MemoryConflict(Exception):
    """The caller's memory revision no longer describes the authoritative scope."""


class MemoryNotFound(Exception):
    """No visible memory exists within the supplied authenticated scope."""


def _memory_scope(tenant_id: str, user_id: str, feature_tag: str) -> None:
    _scope(tenant_id, user_id, feature_tag)
    if not user_id or not feature_tag:
        raise ValueError("A complete memory scope is required")


def _memory_record(row) -> dict:
    item = dict(row)
    for key in ("id", "supersedes_id"):
        if key in item:
            item[key] = str(item[key]) if item[key] is not None else None
    item["source_ids"] = [str(value) for value in item.get("source_ids") or []]
    item["revision"] = item.pop("memory_revision")
    item["status"] = item.pop("memory_status")
    if (item["status"] == "active" and item.get("expires_at") is not None
            and item["expires_at"] <= datetime.now(UTC)):
        item["status"] = "expired"
        item["prompt"] = item["response"] = None
        item["cache_eligible"] = False
    for key in ("created_at", "expires_at"):
        if item.get(key) is not None:
            item[key] = item[key].isoformat()
    if isinstance(item.get("retrieval"), str):
        item["retrieval"] = json.loads(item["retrieval"])
    return item


MEMORY_FIELDS = """
    id, user_id, feature_tag, prompt, response, memory_revision, memory_status,
    memory_kind, created_at, expires_at, source_ids, supersedes_id, retrieval,
    coalesce((memory_status = 'active' AND expires_at > now() AND cache_expires_at > now()
     AND response IS NOT NULL AND embedding IS NOT NULL AND NOT cache_hit),false) AS cache_eligible
"""


def _vec(embedding: list[float]) -> str:
    """Pass pgvector's text format; reject non-finite input before serialization."""
    if not embedding or not all(math.isfinite(x) for x in embedding) or not any(embedding):
        raise ValueError("Embedding must contain finite values and have a nonzero norm")
    return "[" + ",".join(f"{x:.9g}" for x in embedding) + "]"


def _scope(tenant_id: str, user_id: str | None = None, feature_tag: str | None = None) -> None:
    if not tenant_id or tenant_id == QUARANTINE_TENANT:
        raise ValueError("An active tenant scope is required")
    if user_id == "" or feature_tag == "":
        raise ValueError("Memory scope must not be empty")


class Database:
    def __init__(self) -> None:
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=getattr(settings, "database_pool_min_size", 2),
            max_size=getattr(settings, "database_pool_max_size", 10),
            command_timeout=getattr(settings, "database_command_timeout_seconds", 15),
        )
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("SELECT pg_advisory_xact_lock($1)", MIGRATION_LOCK)
                    await conn.execute(
                        "CREATE TABLE IF NOT EXISTS gateway_schema_migrations "
                        "(version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
                    )
                    applied = {r["version"] for r in await conn.fetch(
                        "SELECT version FROM gateway_schema_migrations"
                    )}
                    for path in sorted(MIGRATIONS.glob("[0-9]*.sql")):
                        if path.name in applied:
                            continue
                        sql = path.read_text().replace("{dim}", str(int(settings.embedding_dim)))
                        await conn.execute(sql)
                        await conn.execute(
                            "INSERT INTO gateway_schema_migrations(version) VALUES ($1)", path.name
                        )
                    actual_type = await conn.fetchval(
                        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
                        "WHERE attrelid = 'calls'::regclass AND attname = 'embedding'"
                    )
                    expected_type = f"vector({settings.embedding_dim})"
                    if actual_type != expected_type:
                        raise RuntimeError(
                            f"Embedding schema is {actual_type}, configured {expected_type}. "
                            "Apply a reviewed dimension migration and re-embed before changing EMBEDDING_DIM."
                        )
        except BaseException:
            await self.pool.close()
            self.pool = None
            raise

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def health(self) -> bool:
        if self.pool is None:
            return False
        try:
            return await self.pool.fetchval("SELECT 1") == 1
        except Exception:
            return False

    async def find_exact(
        self,
        prompt: str,
        *,
        tenant_id: str,
        user_id: str,
        feature_tag: str,
        max_tokens: int,
        embedding_space: str,
        generation_config: str,
    ) -> dict | None:
        """Find an eligible exact completion independently of approximate vectors.

        The indexed digest narrows candidates; full prompt equality still decides
        the match. Scope, generation settings and both TTLs remain authoritative.
        """
        _scope(tenant_id, user_id, feature_tag)
        row = await self.pool.fetchrow(
            """
            SELECT id, prompt, response, feature_tag,
                   extract(epoch FROM created_at) AS created_epoch
            FROM calls
            WHERE tenant_id = $2 AND user_id = $3 AND feature_tag = $4
              AND md5(prompt) = md5($1) AND prompt = $1
              AND max_tokens = $5 AND embedding_space = $6 AND generation_config = $7
              AND embedding IS NOT NULL AND prompt IS NOT NULL
              AND response IS NOT NULL AND NOT cache_hit
              AND memory_status = 'active'
              AND expires_at > now() AND cache_expires_at > now()
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """,
            prompt, tenant_id, user_id, feature_tag, max_tokens, embedding_space, generation_config,
        )
        if row is None:
            return None
        return {
            "id": str(row["id"]), "prompt": row["prompt"], "response": row["response"],
            "feature_tag": row["feature_tag"], "created_epoch": float(row["created_epoch"]),
            "similarity": 1.0, "cache_eligible": True,
        }

    async def find_similar(
        self,
        embedding: list[float],
        limit: int,
        min_similarity: float,
        *,
        tenant_id: str = "local",
        user_id: str,
        feature_tag: str,
        max_tokens: int,
        embedding_space: str,
        generation_config: str = "default",
    ) -> list[dict]:
        """Read within the full memory boundary; annotate stricter cache eligibility.

        An older generation budget/configuration can contribute context, but may
        never supply a cached completion for a different generation request.
        """
        _scope(tenant_id, user_id, feature_tag)
        rows = await self.pool.fetch(
            """
            SELECT id, prompt, response, feature_tag,
                   extract(epoch FROM created_at) AS created_epoch,
                   1 - (embedding <=> $1::vector) AS similarity,
                   (max_tokens = $6 AND generation_config = $8
                    AND cache_expires_at > now()) AS cache_eligible
            FROM calls
            WHERE tenant_id = $3 AND user_id = $4 AND feature_tag = $5
              AND embedding_space = $7
              AND embedding IS NOT NULL AND prompt IS NOT NULL
              AND response IS NOT NULL AND NOT cache_hit
              AND memory_status = 'active'
              AND expires_at > now()
            ORDER BY embedding <=> $1::vector
            LIMIT $2
            """,
            _vec(embedding), max(1, min(limit, 200)), tenant_id, user_id,
            feature_tag, max_tokens, embedding_space, generation_config,
        )
        return [
            {
                "id": str(r["id"]), "prompt": r["prompt"], "response": r["response"],
                "feature_tag": r["feature_tag"], "created_epoch": float(r["created_epoch"]),
                "similarity": float(r["similarity"]), "cache_eligible": bool(r["cache_eligible"]),
            }
            for r in rows if r["similarity"] >= min_similarity
        ]

    async def log_call(
        self,
        *,
        call_id: uuid.UUID,
        user_id: str,
        feature_tag: str,
        prompt: str | None,
        response: str | None,
        model: str | None,
        provider: str | None,
        tokens_in: int,
        tokens_out: int,
        cost: float | None,
        latency_ms: int,
        cache_hit: bool,
        fallback_used: bool,
        embedding: list[float] | None,
        tenant_id: str = "local",
        max_tokens: int | None = None,
        embedding_space: str | None = None,
        generation_config: str = "default",
        graph_event: dict | None = None,
        expected_memory_epoch: int | None = None,
        source_ids: list[str] | None = None,
        retrieval: dict | None = None,
    ) -> None:
        """Commit billing and its graph event together; call ID makes retries safe."""
        _scope(tenant_id, user_id, feature_tag)
        created_at = datetime.now(UTC)
        expires_at = created_at + timedelta(seconds=getattr(settings, "memory_ttl_seconds", 2592000))
        cache_expires_at = min(
            expires_at, created_at + timedelta(seconds=getattr(settings, "cache_ttl_seconds", 3600))
        )
        # A content-free accounting row must never smuggle memory via its outbox.
        if prompt is None:
            response, embedding, graph_event = None, None, None
        event = None
        if graph_event is not None:
            if graph_event.get("kind") not in {"call", "cache_hit"}:
                raise ValueError("Unsupported graph event kind")
            payload = dict(graph_event["payload"])
            payload.update(
                call_id=str(call_id), tenant_id=tenant_id, user_id=user_id,
                feature_tag=feature_tag, prompt=prompt,
                created_at=created_at.isoformat(), expires_at=expires_at.isoformat(),
            )
            if graph_event["kind"] == "call":
                payload["response"] = response
            event = json.dumps({"kind": graph_event["kind"], "payload": payload}, allow_nan=False)
        conflict = False
        source_uuids = [uuid.UUID(value) for value in source_ids] if source_ids is not None else None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                if expected_memory_epoch is not None or source_uuids:
                    current = await self._lock_scope(conn, tenant_id, user_id, feature_tag)
                    conflict = expected_memory_epoch is not None and current != expected_memory_epoch
                if source_uuids and not conflict:
                    sources = await conn.fetchrow(
                        "SELECT count(*) AS count,min(expires_at) AS expiry FROM calls "
                        "WHERE id=ANY($1::uuid[]) AND tenant_id=$2 AND user_id=$3 AND feature_tag=$4 "
                        "AND memory_status='active' AND expires_at>clock_timestamp() "
                        "AND prompt IS NOT NULL AND response IS NOT NULL AND NOT cache_hit",
                        list(set(source_uuids)), tenant_id, user_id, feature_tag,
                    )
                    conflict = sources["count"] != len(set(source_uuids))
                    if not conflict:
                        # Derived evidence cannot renew the lifetime of an old fact.
                        expires_at = min(expires_at, sources["expiry"])
                        cache_expires_at = min(cache_expires_at, expires_at)
                        if event is not None:
                            encoded = json.loads(event)
                            encoded["payload"]["expires_at"] = expires_at.isoformat()
                            event = json.dumps(encoded, allow_nan=False)
                if conflict:
                    # A provider may already have charged. Commit accounting,
                    # never its stale answer or a resurrecting graph payload.
                    prompt, response, embedding, event = None, None, None, None
                inserted = await conn.fetchval(
                    """
                    INSERT INTO calls (id, tenant_id, user_id, feature_tag, prompt, response,
                        model, provider, tokens_in, tokens_out, cost, latency_ms, cache_hit,
                        fallback_used, embedding, max_tokens, embedding_space,
                        generation_config, created_at, expires_at, cache_expires_at,
                        source_ids, retrieval, memory_visible)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::vector,
                            $16,$17,$18,$19,$20,$21,$22::uuid[],$23::jsonb,$24)
                    ON CONFLICT (id) DO NOTHING RETURNING id
                    """,
                    call_id, tenant_id, user_id, feature_tag, prompt, response, model,
                    provider, tokens_in, tokens_out, cost, latency_ms, cache_hit,
                    fallback_used, _vec(embedding) if embedding is not None else None,
                    max_tokens, embedding_space, generation_config, created_at,
                    expires_at if prompt is not None else None,
                    cache_expires_at if embedding is not None else None,
                    source_uuids,
                    json.dumps(retrieval or {"sources": [
                        {"id": value} for value in source_ids or []
                    ]}), prompt is not None,
                )
                if inserted is None:
                    existing = await conn.fetchrow(
                        "SELECT tenant_id, user_id, feature_tag,memory_status FROM calls WHERE id = $1",
                        call_id,
                    )
                    scope_keys = ("tenant_id", "user_id", "feature_tag")
                    if existing is None or tuple(existing[k] for k in scope_keys) != (
                        tenant_id, user_id, feature_tag
                    ):
                        raise ValueError("Call ID already belongs to a different scope")
                    conflict = conflict or existing.get("memory_status", "active") != "active"
                    if not conflict:
                        return
                    event = None
                if event is not None:
                    await conn.execute(
                        "INSERT INTO graph_outbox (call_id, tenant_id, event, user_id, feature_tag) "
                        "VALUES ($1,$2,$3::jsonb,$4,$5)",
                        call_id, tenant_id, event, user_id, feature_tag,
                    )
        if conflict:
            raise MemoryConflict("Memory changed while the request was running")

    @staticmethod
    async def _lock_scope(conn, tenant_id: str, user_id: str, feature_tag: str) -> int:
        _memory_scope(tenant_id, user_id, feature_tag)
        await conn.execute(
            "INSERT INTO memory_scopes(tenant_id,user_id,feature_tag) VALUES($1,$2,$3) "
            "ON CONFLICT DO NOTHING", tenant_id, user_id, feature_tag,
        )
        return await conn.fetchval(
            "SELECT revision FROM memory_scopes WHERE tenant_id=$1 AND user_id=$2 "
            "AND feature_tag=$3 FOR UPDATE", tenant_id, user_id, feature_tag,
        )

    async def memory_epoch(self, *, tenant_id: str, user_id: str, feature_tag: str) -> int:
        _memory_scope(tenant_id, user_id, feature_tag)
        return await self.pool.fetchval(
            "SELECT revision FROM memory_scopes WHERE tenant_id=$1 AND user_id=$2 AND feature_tag=$3",
            tenant_id, user_id, feature_tag,
        ) or 0

    async def filter_active_memories(
        self, ids: list[str], *, tenant_id: str, user_id: str, feature_tag: str,
    ) -> list[dict]:
        """Hydrate graph candidates from the authority, never trust projected content."""
        _memory_scope(tenant_id, user_id, feature_tag)
        if not ids:
            return []
        rows = await self.pool.fetch(
            "SELECT id,prompt,response,feature_tag,extract(epoch FROM created_at) AS created_epoch "
            "FROM calls WHERE id=ANY($1::uuid[]) AND tenant_id=$2 AND user_id=$3 AND feature_tag=$4 "
            "AND memory_status='active' AND expires_at>now() AND prompt IS NOT NULL "
            "AND response IS NOT NULL AND NOT cache_hit",
            [uuid.UUID(value) for value in ids[:400]], tenant_id, user_id, feature_tag,
        )
        return [{**dict(row), "id": str(row["id"]), "created_epoch": float(row["created_epoch"])}
                for row in rows]

    async def list_memories(
        self, *, tenant_id: str, user_id: str, feature_tag: str,
        limit: int = 50, cursor: str | None = None,
    ) -> dict:
        _memory_scope(tenant_id, user_id, feature_tag)
        size = max(1, min(limit, 100))
        scope_digest = hashlib.sha256(json.dumps([tenant_id, user_id, feature_tag]).encode()).hexdigest()
        async with self.pool.acquire() as conn:
            async with conn.transaction(isolation="repeatable_read", readonly=True):
                revision = await conn.fetchval(
                    "SELECT revision FROM memory_scopes WHERE tenant_id=$1 AND user_id=$2 AND feature_tag=$3",
                    tenant_id, user_id, feature_tag,
                ) or 0
                snapshot = datetime.now(UTC)
                after_time, after_id = None, None
                if cursor:
                    try:
                        payload = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
                        if payload["scope"] != scope_digest:
                            raise ValueError("Cursor belongs to another memory scope")
                        if payload["revision"] != revision:
                            raise MemoryConflict("Memory changed; restart pagination")
                        snapshot = datetime.fromisoformat(payload["snapshot"])
                        after_time = datetime.fromisoformat(payload["after_time"])
                        after_id = uuid.UUID(payload["after_id"])
                        if snapshot.tzinfo is None or after_time.tzinfo is None:
                            raise ValueError("Cursor timestamps require a timezone")
                    except MemoryConflict:
                        raise
                    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
                        raise ValueError("Invalid memory cursor") from exc
                rows = await conn.fetch(
                    f"SELECT {MEMORY_FIELDS} FROM calls "
                    "WHERE tenant_id=$1 AND user_id=$2 AND feature_tag=$3 AND memory_visible "
                    "AND created_at <= $4 AND ($5::timestamptz IS NULL OR (created_at,id)<($5,$6::uuid)) "
                    "ORDER BY created_at DESC,id DESC LIMIT $7",
                    tenant_id, user_id, feature_tag, snapshot, after_time, after_id, size + 1,
                )
        next_cursor = None
        if len(rows) > size:
            tail = rows[size - 1]
            next_cursor = base64.urlsafe_b64encode(json.dumps({
                "scope": scope_digest, "revision": revision, "snapshot": snapshot.isoformat(),
                "after_time": tail["created_at"].isoformat(), "after_id": str(tail["id"]),
            }, separators=(",", ":")).encode()).decode()
        return {"items": [_memory_record(row) for row in rows[:size]],
                "next_cursor": next_cursor, "scope_revision": revision}

    async def memory_detail(
        self, memory_id: uuid.UUID, *, tenant_id: str, user_id: str, feature_tag: str,
    ) -> dict:
        _memory_scope(tenant_id, user_id, feature_tag)
        row = await self.pool.fetchrow(
            f"SELECT {MEMORY_FIELDS} FROM calls WHERE id=$1 AND tenant_id=$2 "
            "AND user_id=$3 AND feature_tag=$4 AND memory_visible",
            memory_id, tenant_id, user_id, feature_tag,
        )
        if row is None:
            raise MemoryNotFound()
        return _memory_record(row)

    @staticmethod
    async def _advance_scope(conn, tenant_id, user_id, feature_tag) -> int:
        # Any mutation can change an answer to an unchanged question.
        await conn.execute(
            "UPDATE calls SET cache_expires_at=now() WHERE tenant_id=$1 AND user_id=$2 AND feature_tag=$3",
            tenant_id, user_id, feature_tag,
        )
        return await conn.fetchval(
            "UPDATE memory_scopes SET revision=revision+1 WHERE tenant_id=$1 AND user_id=$2 "
            "AND feature_tag=$3 RETURNING revision", tenant_id, user_id, feature_tag,
        )

    @staticmethod
    async def _retire_memories(conn, tenant_id, user_id, feature_tag, target=None, supersede=False) -> dict:
        # Unknown legacy provenance is conservatively retired in this scope.
        # New records carry explicit [] or source IDs, allowing exact transitive invalidation.
        row = await conn.fetchrow("""
            WITH RECURSIVE affected(id) AS (
                SELECT id FROM calls WHERE tenant_id=$1 AND user_id=$2 AND feature_tag=$3
                  AND memory_visible AND memory_status='active'
                  AND ($4::uuid IS NULL OR id=$4 OR (source_ids IS NULL AND memory_kind='generated'))
                UNION
                SELECT c.id FROM calls c JOIN affected a ON a.id=ANY(c.source_ids)
                WHERE c.tenant_id=$1 AND c.user_id=$2 AND c.feature_tag=$3
                  AND c.memory_visible AND c.memory_status='active'
            ), retired AS (
                UPDATE calls c SET prompt=NULL,response=NULL,embedding=NULL,
                    memory_revision=memory_revision+1,expires_at=now(),cache_expires_at=now(),
                    memory_status=CASE WHEN c.id=$4 AND $5 THEN 'superseded'
                                       WHEN $4::uuid IS NULL OR c.id=$4 THEN 'deleted'
                                       ELSE 'invalidated' END
                FROM affected a WHERE c.id=a.id
                RETURNING c.id,c.memory_revision
            ), scrubbed AS (
                DELETE FROM graph_outbox o USING retired r WHERE o.call_id=r.id RETURNING o.id
            ), queued AS (
                INSERT INTO graph_outbox(call_id,tenant_id,user_id,feature_tag,event)
                SELECT r.id,$1,$2,$3,jsonb_build_object('kind','delete','payload',jsonb_build_object(
                    'call_id',r.id::text,'tenant_id',$1::text,'user_id',$2::text,'feature_tag',$3::text,
                    'memory_revision',r.memory_revision))
                FROM retired r CROSS JOIN (SELECT count(*) FROM scrubbed) AS applied
                RETURNING call_id
            )
            SELECT count(*) AS count,
                (SELECT array_agg(call_id) FROM
                 (SELECT call_id FROM queued ORDER BY call_id LIMIT 1000) ids) AS ids
            FROM queued
        """, tenant_id, user_id, feature_tag, target, supersede)
        return {"deleted_ids": [str(value) for value in row["ids"] or []],
                "deleted_count": row["count"], "deleted_ids_truncated": row["count"] > 1000,
                "invalidated_count": max(0, row["count"] - (1 if target is not None else 0))}

    async def create_memory(
        self, *, tenant_id: str, user_id: str, feature_tag: str, prompt: str, response: str,
        embedding: list[float], embedding_space: str, expected_scope_revision: int | None = None,
        supersedes_id: uuid.UUID | None = None, expected_revision: int | None = None,
    ) -> dict:
        """Create curated evidence, optionally superseding old evidence, without generation."""
        _memory_scope(tenant_id, user_id, feature_tag)
        memory_id = uuid.uuid4()
        created = datetime.now(UTC)
        expiry = created + timedelta(seconds=settings.memory_ttl_seconds)
        retired = {"invalidated_count": 0}
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                revision = await self._lock_scope(conn, tenant_id, user_id, feature_tag)
                if supersedes_id is None:
                    if expected_scope_revision != revision:
                        raise MemoryConflict("Scope revision changed")
                else:
                    old = await conn.fetchrow(
                        "SELECT memory_revision,memory_status,expires_at FROM calls WHERE id=$1 "
                        "AND tenant_id=$2 AND user_id=$3 AND feature_tag=$4 AND memory_visible FOR UPDATE",
                        supersedes_id, tenant_id, user_id, feature_tag,
                    )
                    if old is None:
                        raise MemoryNotFound()
                    if (old["memory_revision"] != expected_revision or old["memory_status"] != "active"
                            or old["expires_at"] is None or old["expires_at"] <= created):
                        raise MemoryConflict("Memory revision changed or memory is no longer active")
                    retired = await self._retire_memories(
                        conn, tenant_id, user_id, feature_tag, supersedes_id, True,
                    )
                revision = await self._advance_scope(conn, tenant_id, user_id, feature_tag)
                neighbor_rows = await conn.fetch(
                    "SELECT id,1-(embedding <=> $1::vector) AS score FROM calls "
                    "WHERE tenant_id=$2 AND user_id=$3 AND feature_tag=$4 AND embedding_space=$5 "
                    "AND memory_status='active' AND expires_at>now() AND response IS NOT NULL "
                    "AND embedding IS NOT NULL AND NOT cache_hit "
                    "ORDER BY embedding <=> $1::vector LIMIT $6",
                    _vec(embedding), tenant_id, user_id, feature_tag, embedding_space,
                    min(settings.graph_candidate_pool, 200),
                )
                neighbors = [
                    {"id": str(value["id"]), "score": float(value["score"])}
                    for value in neighbor_rows if value["score"] >= settings.graph_similarity_threshold
                ]
                row = await conn.fetchrow(
                    "INSERT INTO calls(id,tenant_id,user_id,feature_tag,prompt,response,model,provider,"
                    "embedding,embedding_space,generation_config,created_at,expires_at,source_ids,"
                    "supersedes_id,memory_kind,memory_visible,retrieval) "
                    "VALUES($1,$2,$3,$4,$5,$6,'curated','user',$7::vector,$8,'curated',$9,$10,"
                    "'{}'::uuid[],$11,'curated',true,'{\"sources\":[]}'::jsonb) "
                    f"RETURNING {MEMORY_FIELDS}",
                    memory_id, tenant_id, user_id, feature_tag, prompt, response,
                    _vec(embedding), embedding_space, created, expiry, supersedes_id,
                )
                payload = {
                    "call_id": str(memory_id), "tenant_id": tenant_id, "user_id": user_id,
                    "feature_tag": feature_tag, "prompt": prompt, "response": response,
                    "model": "curated", "provider": "user", "fallback_provider": None,
                    "tokens_in": 0, "tokens_out": 0, "cost": 0, "latency_ms": 0,
                    "similar": neighbors, "informed_by": [], "memory_revision": 1,
                    "created_at": created.isoformat(), "expires_at": expiry.isoformat(),
                }
                await conn.execute(
                    "INSERT INTO graph_outbox(call_id,tenant_id,user_id,feature_tag,event) "
                    "VALUES($1,$2,$3,$4,$5::jsonb)", memory_id, tenant_id, user_id, feature_tag,
                    json.dumps({"kind": "call", "payload": payload}),
                )
        return {"memory": _memory_record(row), "scope_revision": revision,
                "invalidated_count": retired["invalidated_count"], "graph_write": "queued"}

    async def delete_memory(
        self, memory_id: uuid.UUID | None, *, tenant_id: str, user_id: str, feature_tag: str,
        expected_revision: int,
    ) -> dict:
        _memory_scope(tenant_id, user_id, feature_tag)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                revision = await self._lock_scope(conn, tenant_id, user_id, feature_tag)
                if memory_id is None:
                    if revision != expected_revision:
                        raise MemoryConflict("Scope revision changed")
                else:
                    row = await conn.fetchrow(
                        "SELECT memory_revision,memory_status FROM calls WHERE id=$1 AND tenant_id=$2 "
                        "AND user_id=$3 AND feature_tag=$4 AND memory_visible FOR UPDATE",
                        memory_id, tenant_id, user_id, feature_tag,
                    )
                    if row is None:
                        raise MemoryNotFound()
                    if row["memory_revision"] != expected_revision:
                        raise MemoryConflict("Memory revision changed")
                    if row["memory_status"] != "active":
                        return {"deleted_ids": [], "deleted_count": 0, "invalidated_count": 0,
                                "deleted_ids_truncated": False, "scope_revision": revision,
                                "graph_write": "queued"}
                result = await self._retire_memories(conn, tenant_id, user_id, feature_tag, memory_id)
                revision = await self._advance_scope(conn, tenant_id, user_id, feature_tag)
        return {**result, "scope_revision": revision, "graph_write": "queued"}

    async def claim_graph_events(self, limit: int = 32, lease_seconds: int = 60) -> list[dict]:
        """Claim a bounded batch; SKIP LOCKED allows safe competing workers."""
        lease_token = uuid.uuid4()
        rows = await self.pool.fetch(
            """
            WITH next_events AS (
                SELECT candidate.id FROM graph_outbox AS candidate
                WHERE candidate.available_at <= now()
                  AND (candidate.lease_until IS NULL OR candidate.lease_until < now())
                  AND NOT EXISTS (
                      SELECT 1 FROM graph_outbox AS earlier
                      WHERE earlier.tenant_id = candidate.tenant_id
                        AND earlier.user_id = candidate.user_id
                        AND earlier.feature_tag = candidate.feature_tag
                        AND earlier.id < candidate.id
                  )
                ORDER BY candidate.available_at, candidate.id
                LIMIT $1 FOR UPDATE SKIP LOCKED
            )
            UPDATE graph_outbox AS o
            SET lease_until = now() + $2 * interval '1 second', lease_token = $3,
                attempts = o.attempts + 1
            FROM next_events AS n WHERE o.id = n.id
            RETURNING o.id, o.call_id, o.event, o.lease_token, o.attempts
            """,
            max(1, min(limit, 200)), max(5, min(lease_seconds, 3600)), lease_token,
        )
        events = []
        for row in rows:
            item = dict(row)
            if isinstance(item["event"], str):
                item["event"] = json.loads(item["event"])
            events.append(item)
        return events

    async def ack_graph_event(self, event_id: int, lease_token: uuid.UUID) -> None:
        await self.pool.execute(
            "DELETE FROM graph_outbox WHERE id = $1 AND lease_token = $2", event_id, lease_token
        )

    async def retry_graph_event(
        self, event_id: int, lease_token: uuid.UUID, error: str, retry_delay_seconds: float = 2
    ) -> None:
        # Store only a bounded error category, never provider payloads or prompts.
        await self.pool.execute(
            "UPDATE graph_outbox SET lease_until = NULL, lease_token = NULL, last_error = $3, "
            "available_at = now() + $4 * interval '1 second' WHERE id = $1 AND lease_token = $2",
            event_id, lease_token, error[:120], max(1, min(retry_delay_seconds, 300)),
        )

    async def outbox_pending(self) -> int:
        return await self.pool.fetchval("SELECT count(*) FROM graph_outbox")

    async def outbox_status(self) -> dict:
        row = await self.pool.fetchrow(
            "SELECT count(*) AS pending, count(*) FILTER (WHERE last_error IS NOT NULL) AS failed, "
            "coalesce(extract(epoch FROM now() - min(created_at)), 0) AS oldest_age_seconds "
            "FROM graph_outbox"
        )
        return {
            "pending": row["pending"], "failed": row["failed"],
            "oldest_age_seconds": max(0.0, float(row["oldest_age_seconds"])),
        }

    async def purge_expired_memory(self, limit: int = 1000) -> int:
        """Clear expired content and queued payloads without deleting billing history."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    "SELECT id FROM calls WHERE expires_at <= now() AND prompt IS NOT NULL "
                    "ORDER BY expires_at LIMIT $1 FOR UPDATE SKIP LOCKED", max(1, min(limit, 10000))
                )
                ids = [r["id"] for r in rows]
                if ids:
                    await conn.execute("DELETE FROM graph_outbox WHERE call_id = ANY($1::uuid[])", ids)
                    await conn.execute(
                        "UPDATE calls SET prompt = NULL, response = NULL, embedding = NULL "
                        "WHERE id = ANY($1::uuid[])", ids
                    )
                return len(ids)

    async def usage_rollup(
        self, user_id: str | None = None, feature_tag: str | None = None,
        *, tenant_id: str = "local", limit: int = 100,
    ) -> list[dict]:
        _scope(tenant_id, user_id, feature_tag)
        conditions, params = ["tenant_id = $1", "memory_kind <> 'curated'"], [tenant_id]
        for name, value in (("user_id", user_id), ("feature_tag", feature_tag)):
            if value is not None:
                params.append(value)
                conditions.append(f"{name} = ${len(params)}")
        params.append(max(1, min(limit, 1000)))
        rows = await self.pool.fetch(
            f"""
            SELECT user_id, feature_tag, date_trunc('day', created_at) AS day,
                   count(*) AS calls, count(*) FILTER (WHERE cache_hit) AS cache_hits,
                   count(*) FILTER (WHERE cost IS NULL) AS unpriced_calls,
                   coalesce(sum(tokens_in), 0) AS tokens_in,
                   coalesce(sum(tokens_out), 0) AS tokens_out,
                   coalesce(sum(cost), 0) AS cost,
                   coalesce(avg(latency_ms), 0) AS avg_latency_ms
            FROM calls WHERE {' AND '.join(conditions)}
            GROUP BY user_id, feature_tag, day
            ORDER BY day DESC, user_id, feature_tag LIMIT ${len(params)}
            """,
            *params,
        )
        return [
            {
                **{k: r[k] for k in ("user_id", "feature_tag", "calls", "cache_hits", "tokens_in",
                                     "tokens_out", "unpriced_calls")},
                "day": r["day"].date().isoformat() if isinstance(r["day"], datetime) else str(r["day"]),
                "cost": float(r["cost"]), "avg_latency_ms": float(r["avg_latency_ms"]),
            }
            for r in rows
        ]

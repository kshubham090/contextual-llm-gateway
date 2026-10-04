"""Tenant-scoped pgvector memory, billing records, and a transactional graph outbox."""

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
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                inserted = await conn.fetchval(
                    """
                    INSERT INTO calls (id, tenant_id, user_id, feature_tag, prompt, response,
                        model, provider, tokens_in, tokens_out, cost, latency_ms, cache_hit,
                        fallback_used, embedding, max_tokens, embedding_space,
                        generation_config, created_at, expires_at, cache_expires_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::vector,
                            $16,$17,$18,$19,$20,$21)
                    ON CONFLICT (id) DO NOTHING RETURNING id
                    """,
                    call_id, tenant_id, user_id, feature_tag, prompt, response, model,
                    provider, tokens_in, tokens_out, cost, latency_ms, cache_hit,
                    fallback_used, _vec(embedding) if embedding is not None else None,
                    max_tokens, embedding_space, generation_config, created_at,
                    expires_at if prompt is not None else None,
                    cache_expires_at if embedding is not None else None,
                )
                if inserted is None:
                    existing = await conn.fetchrow(
                        "SELECT tenant_id, user_id, feature_tag FROM calls WHERE id = $1", call_id
                    )
                    scope_keys = ("tenant_id", "user_id", "feature_tag")
                    if existing is None or tuple(existing[k] for k in scope_keys) != (
                        tenant_id, user_id, feature_tag
                    ):
                        raise ValueError("Call ID already belongs to a different scope")
                    return
                if event is not None:
                    await conn.execute(
                        "INSERT INTO graph_outbox (call_id, tenant_id, event, user_id, feature_tag) "
                        "VALUES ($1,$2,$3::jsonb,$4,$5) "
                        "ON CONFLICT (call_id) DO NOTHING",
                        call_id, tenant_id, event, user_id, feature_tag,
                    )

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
        conditions, params = ["tenant_id = $1"], [tenant_id]
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

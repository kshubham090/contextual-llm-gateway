"""Idempotent, tenant/user/feature-isolated Neo4j memory projection.

Gateway labels intentionally exclude the legacy, unscoped graph. Every memory
edge and every node on a traversed path must satisfy the same scope and TTL.
"""

import hashlib
import json
from datetime import UTC, datetime, timedelta

from neo4j import AsyncGraphDatabase, Query, unit_of_work

from .config import settings
from .db import _scope

CONSTRAINTS = [
    f"CREATE CONSTRAINT gateway_{label.lower()}_key IF NOT EXISTS "
    f"FOR (n:Gateway{label}) REQUIRE n.key IS UNIQUE"
    for label in ("Call", "User", "Feature", "Model", "Provider")
] + [
    "CREATE INDEX gateway_call_scope IF NOT EXISTS FOR (c:GatewayCall) "
    "ON (c.tenant_id, c.user_id, c.feature_tag)",
    "CREATE INDEX gateway_call_expiry IF NOT EXISTS FOR (c:GatewayCall) ON (c.expires_at)",
]


def _key(*parts: str) -> str:
    return hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()


def _times(created_at: str | None, expires_at: str | None) -> tuple[str, str, bool]:
    created = datetime.fromisoformat(created_at) if created_at else datetime.now(UTC)
    expiry = datetime.fromisoformat(expires_at) if expires_at else created + timedelta(
        seconds=getattr(settings, "memory_ttl_seconds", 2592000)
    )
    if created.tzinfo is None or expiry.tzinfo is None:
        raise ValueError("Memory timestamps must include a timezone")
    return created.isoformat(), expiry.isoformat(), expiry <= datetime.now(UTC)


class MemoryGraph:
    def __init__(self) -> None:
        self.driver = None

    async def connect(self) -> None:
        self.driver = AsyncGraphDatabase.driver(
            settings.neo4j_uri,
            auth=(settings.neo4j_user, settings.neo4j_password),
            connection_timeout=getattr(settings, "graph_timeout_seconds", 5),
            max_transaction_retry_time=getattr(settings, "graph_timeout_seconds", 5),
        )
        try:
            await self.driver.verify_connectivity()
            async with self.driver.session() as session:
                for stmt in CONSTRAINTS:
                    result = await session.run(self._query(stmt))
                    await result.consume()
        except BaseException:
            await self.driver.close()
            self.driver = None
            raise

    @staticmethod
    def _query(text: str) -> Query:
        return Query(text, timeout=getattr(settings, "graph_timeout_seconds", 5))

    async def close(self) -> None:
        if self.driver:
            await self.driver.close()
            self.driver = None

    async def health(self) -> bool:
        if self.driver is None:
            return False
        try:
            async with self.driver.session() as session:
                result = await session.run(self._query("RETURN 1 AS healthy"))
                record = await result.single()
                return record is not None and record["healthy"] == 1
        except Exception:
            return False

    async def expand_neighborhood(
        self, seed_ids: list[str], limit: int, *, tenant_id: str = "local",
        user_id: str, feature_tag: str,
    ) -> list[dict]:
        _scope(tenant_id, user_id, feature_tag)
        if not seed_ids:
            return []
        async with self.driver.session() as session:
            result = await session.run(
                self._query("""
                MATCH (c:GatewayCall)
                WHERE c.id IN $seed_ids AND c.tenant_id = $tenant_id
                  AND c.user_id = $user_id AND c.feature_tag = $feature_tag
                  AND c.expires_at > datetime()
                MATCH path = (c)-[:SIMILAR_TO|INFORMED_BY*0..2]-(n:GatewayCall)
                WHERE all(node IN nodes(path) WHERE node:GatewayCall
                          AND node.tenant_id = $tenant_id AND node.user_id = $user_id
                          AND node.feature_tag = $feature_tag AND node.expires_at > datetime())
                  AND all(edge IN relationships(path) WHERE edge.tenant_id = $tenant_id
                          AND edge.user_id = $user_id AND edge.feature_tag = $feature_tag
                          AND edge.expires_at > datetime())
                WITH DISTINCT n
                WHERE n.prompt IS NOT NULL AND n.response IS NOT NULL
                RETURN n.id AS id, n.prompt AS prompt, n.response AS response,
                       n.feature_tag AS feature_tag, n.created_at.epochSeconds AS created_epoch
                ORDER BY n.created_at DESC LIMIT $limit
                """),
                seed_ids=seed_ids[:200], limit=max(1, min(limit, 200)),
                tenant_id=tenant_id, user_id=user_id, feature_tag=feature_tag,
            )
            return [dict(record) async for record in result]

    async def _write(self, statements: list[tuple[str, dict]]) -> None:
        @unit_of_work(timeout=getattr(settings, "graph_timeout_seconds", 5))
        async def transaction(tx):
            for statement, parameters in statements:
                result = await tx.run(statement, **parameters)
                await result.consume()

        async with self.driver.session() as session:
            await session.execute_write(transaction)

    async def write_call(
        self, *, call_id: str, user_id: str, feature_tag: str, prompt: str,
        response: str, model: str, provider: str, fallback_provider: str | None,
        tokens_in: int, tokens_out: int, cost: float | None, latency_ms: int,
        similar: list[dict], informed_by: list[str], tenant_id: str = "local",
        created_at: str | None = None, expires_at: str | None = None,
    ) -> None:
        _scope(tenant_id, user_id, feature_tag)
        created_at, expires_at, expired = _times(created_at, expires_at)
        if expired:
            return
        params = dict(
            call_id=call_id, user_id=user_id, tenant_id=tenant_id, feature_tag=feature_tag,
            call_key=_key(tenant_id, user_id, feature_tag, call_id),
            user_key=_key(tenant_id, user_id, feature_tag, "user"),
            feature_key=_key(tenant_id, user_id, feature_tag, "feature"),
            model_key=_key(tenant_id, user_id, feature_tag, "model", model),
            provider_key=_key(tenant_id, user_id, feature_tag, "provider", provider),
            prompt=prompt, response=response, model=model, provider=provider,
            tokens_in=tokens_in, tokens_out=tokens_out, cost=cost, latency_ms=latency_ms,
            created_at=created_at, expires_at=expires_at,
        )
        statements = [("""
            MERGE (c:GatewayCall {key: $call_key})
            ON CREATE SET c.id = $call_id, c.tenant_id = $tenant_id, c.user_id = $user_id,
                c.feature_tag = $feature_tag, c.prompt = $prompt, c.response = $response,
                c.tokens_in = $tokens_in, c.tokens_out = $tokens_out, c.cost = $cost,
                c.latency_ms = $latency_ms, c.created_at = datetime($created_at),
                c.expires_at = datetime($expires_at), c.cache_hit = false
            MERGE (u:GatewayUser {key: $user_key})
            SET u.id = $user_id, u.tenant_id = $tenant_id, u.user_id = $user_id,
                u.feature_tag = $feature_tag
            MERGE (f:GatewayFeature {key: $feature_key})
            SET f.name = $feature_tag, f.tenant_id = $tenant_id, f.user_id = $user_id,
                f.feature_tag = $feature_tag
            MERGE (m:GatewayModel {key: $model_key})
            SET m.name = $model, m.tenant_id = $tenant_id, m.user_id = $user_id,
                m.feature_tag = $feature_tag
            MERGE (p:GatewayProvider {key: $provider_key})
            SET p.name = $provider, p.tenant_id = $tenant_id, p.user_id = $user_id,
                p.feature_tag = $feature_tag
            MERGE (u)-[made:MADE]->(c)
            MERGE (c)-[tagged:TAGGED]->(f)
            MERGE (c)-[used:USED]->(m)
            MERGE (c)-[routed:ROUTED_TO]->(p)
            FOREACH (r IN [made, tagged, used, routed] |
                SET r.tenant_id = $tenant_id, r.user_id = $user_id,
                    r.feature_tag = $feature_tag, r.expires_at = c.expires_at)
        """, params)]
        if fallback_provider:
            statements.append(("""
                MATCH (c:GatewayCall {key: $call_key})
                MERGE (p:GatewayProvider {key: $fallback_key})
                SET p.name = $fallback_provider, p.tenant_id = $tenant_id,
                    p.user_id = $user_id, p.feature_tag = $feature_tag
                MERGE (c)-[r:FAILED_OVER_TO]->(p)
                SET r.tenant_id = $tenant_id, r.user_id = $user_id,
                    r.feature_tag = $feature_tag, r.expires_at = c.expires_at
            """, {**params, "fallback_provider": fallback_provider,
                    "fallback_key": _key(tenant_id, user_id, feature_tag, "provider", fallback_provider)}))
        for relation, candidates in (
            ("SIMILAR_TO", [{"id": s["id"], "score": s["score"]} for s in similar[:200]]),
            ("INFORMED_BY", [{"id": item, "score": None} for item in informed_by[:200]]),
        ):
            if candidates:
                statements.append((f"""
                    MATCH (c:GatewayCall {{key: $call_key}})
                    UNWIND $candidates AS candidate
                    MATCH (o:GatewayCall {{id: candidate.id, tenant_id: $tenant_id,
                                          user_id: $user_id, feature_tag: $feature_tag}})
                    WHERE o.expires_at > datetime() AND o.key <> c.key
                    MERGE (c)-[r:{relation}]->(o)
                    ON CREATE SET r.score = candidate.score, r.tenant_id = $tenant_id,
                        r.user_id = $user_id, r.feature_tag = $feature_tag,
                        r.expires_at = CASE WHEN c.expires_at < o.expires_at
                                            THEN c.expires_at ELSE o.expires_at END
                """, {**params, "candidates": candidates}))
        await self._write(statements)

    async def write_cache_hit(
        self, *, call_id: str, user_id: str, feature_tag: str, prompt: str,
        cached_call_id: str, similarity: float, tenant_id: str = "local",
        created_at: str | None = None, expires_at: str | None = None,
    ) -> None:
        _scope(tenant_id, user_id, feature_tag)
        created_at, expires_at, expired = _times(created_at, expires_at)
        if expired:
            return
        await self._write([("""
            MERGE (c:GatewayCall {key: $call_key})
            ON CREATE SET c.id = $call_id, c.tenant_id = $tenant_id, c.user_id = $user_id,
                c.feature_tag = $feature_tag, c.prompt = $prompt, c.cache_hit = true,
                c.created_at = datetime($created_at), c.expires_at = datetime($expires_at)
            MERGE (u:GatewayUser {key: $user_key})
            SET u.id = $user_id, u.tenant_id = $tenant_id, u.user_id = $user_id,
                u.feature_tag = $feature_tag
            MERGE (f:GatewayFeature {key: $feature_key})
            SET f.name = $feature_tag, f.tenant_id = $tenant_id, f.user_id = $user_id,
                f.feature_tag = $feature_tag
            MERGE (u)-[made:MADE]->(c)
            MERGE (c)-[tagged:TAGGED]->(f)
            FOREACH (r IN [made, tagged] | SET r.tenant_id = $tenant_id,
                r.user_id = $user_id, r.feature_tag = $feature_tag, r.expires_at = c.expires_at)
            WITH c
            MATCH (o:GatewayCall {id: $cached_call_id, tenant_id: $tenant_id,
                                  user_id: $user_id, feature_tag: $feature_tag})
            WHERE o.expires_at > datetime()
            MERGE (c)-[r:SERVED_FROM_CACHE]->(o)
            ON CREATE SET r.score = $similarity, r.tenant_id = $tenant_id,
                r.user_id = $user_id, r.feature_tag = $feature_tag,
                r.expires_at = CASE WHEN c.expires_at < o.expires_at
                                    THEN c.expires_at ELSE o.expires_at END
        """, dict(
            call_id=call_id, tenant_id=tenant_id, user_id=user_id, feature_tag=feature_tag,
            call_key=_key(tenant_id, user_id, feature_tag, call_id),
            user_key=_key(tenant_id, user_id, feature_tag, "user"),
            feature_key=_key(tenant_id, user_id, feature_tag, "feature"),
            prompt=prompt, cached_call_id=cached_call_id, similarity=similarity,
            created_at=created_at, expires_at=expires_at,
        ))])

    async def stats(
        self, *, tenant_id: str = "local", user_id: str | None = None, feature_tag: str | None = None
    ) -> dict:
        _scope(tenant_id, user_id, feature_tag)
        scope = """
            c.tenant_id = $tenant_id AND c.expires_at > datetime()
            AND ($user_id IS NULL OR c.user_id = $user_id)
            AND ($feature_tag IS NULL OR c.feature_tag = $feature_tag)
        """
        # Aggregate each edge type independently, avoiding a Cartesian product.
        statement = f"""
            CALL {{ MATCH (c:GatewayCall) WHERE {scope} RETURN count(c) AS calls }}
            CALL {{ MATCH (c:GatewayCall)-[r:SIMILAR_TO]->(o:GatewayCall)
                WHERE {scope} AND o.tenant_id = c.tenant_id AND o.user_id = c.user_id
                  AND o.feature_tag = c.feature_tag AND o.expires_at > datetime()
                  AND r.tenant_id = c.tenant_id AND r.user_id = c.user_id
                  AND r.feature_tag = c.feature_tag AND r.expires_at > datetime()
                RETURN count(r) AS similar_edges }}
            CALL {{ MATCH (c:GatewayCall)-[r:INFORMED_BY]->(o:GatewayCall)
                WHERE {scope} AND o.tenant_id = c.tenant_id AND o.user_id = c.user_id
                  AND o.feature_tag = c.feature_tag AND o.expires_at > datetime()
                  AND r.tenant_id = c.tenant_id AND r.user_id = c.user_id
                  AND r.feature_tag = c.feature_tag AND r.expires_at > datetime()
                RETURN count(r) AS informed_by_edges }}
            RETURN calls, similar_edges, informed_by_edges
        """
        async with self.driver.session() as session:
            result = await session.run(
                self._query(statement), tenant_id=tenant_id, user_id=user_id, feature_tag=feature_tag
            )
            record = await result.single()
            return dict(record) if record else {"calls": 0, "similar_edges": 0, "informed_by_edges": 0}

    async def purge_expired_memory(self, limit: int = 1000) -> int:
        bound = max(1, min(limit, 10000))

        @unit_of_work(timeout=getattr(settings, "graph_timeout_seconds", 5))
        async def transaction(tx):
            result = await tx.run("""
                MATCH (c:GatewayCall) WHERE c.expires_at <= datetime()
                WITH c LIMIT $limit
                DETACH DELETE c RETURN count(*) AS deleted
            """, limit=bound)
            record = await result.single()
            # Remove orphan scope identifiers after their last call expires.
            # DELETE (without DETACH) will not remove a concurrent live relation.
            result = await tx.run("""
                MATCH (n)
                WHERE (n:GatewayUser OR n:GatewayFeature OR n:GatewayModel OR n:GatewayProvider)
                  AND NOT (n)--()
                WITH n LIMIT $limit DELETE n
            """, limit=bound)
            await result.consume()
            return record["deleted"] if record else 0

        async with self.driver.session() as session:
            return await session.execute_write(transaction)

"""Neo4j memory graph: every call becomes a node, connected to its user,
feature, model, provider, and semantically related past calls."""
from neo4j import AsyncGraphDatabase

from .config import settings

CONSTRAINTS = [
    "CREATE CONSTRAINT call_id IF NOT EXISTS FOR (c:Call) REQUIRE c.id IS UNIQUE",
    "CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE",
    "CREATE CONSTRAINT feature_name IF NOT EXISTS FOR (f:Feature) REQUIRE f.name IS UNIQUE",
    "CREATE CONSTRAINT model_name IF NOT EXISTS FOR (m:Model) REQUIRE m.name IS UNIQUE",
    "CREATE CONSTRAINT provider_name IF NOT EXISTS FOR (p:Provider) REQUIRE p.name IS UNIQUE",
]


class MemoryGraph:
    def __init__(self) -> None:
        self.driver = None

    async def connect(self) -> None:
        self.driver = AsyncGraphDatabase.driver(
            settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password)
        )
        async with self.driver.session() as session:
            for stmt in CONSTRAINTS:
                await session.run(stmt)

    async def close(self) -> None:
        if self.driver:
            await self.driver.close()

    async def expand_neighborhood(self, seed_ids: list[str], limit: int) -> list[dict]:
        """Walk 1–2 hops out from the vector-similar seed calls.

        The seeds come from pgvector (semantically similar). The graph walk adds
        calls the vector search alone would miss: follow-ups that informed the
        seeds, and calls the seeds were similar to — i.e. the topical cluster.
        """
        if not seed_ids:
            return []
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (c:Call) WHERE c.id IN $seed_ids
                OPTIONAL MATCH (c)-[:SIMILAR_TO|INFORMED_BY*1..2]-(n:Call)
                WITH collect(DISTINCT c) + collect(DISTINCT n) AS nodes
                UNWIND nodes AS node
                WITH DISTINCT node WHERE node IS NOT NULL
                RETURN node.id AS id, node.prompt AS prompt,
                       node.response AS response, node.feature_tag AS feature_tag
                ORDER BY node.created_at DESC
                LIMIT $limit
                """,
                seed_ids=seed_ids,
                limit=limit,
            )
            return [dict(record) async for record in result]

    async def write_call(
        self,
        *,
        call_id: str,
        user_id: str,
        feature_tag: str,
        prompt: str,
        response: str,
        model: str,
        provider: str,
        fallback_provider: str | None,
        tokens_in: int,
        tokens_out: int,
        cost: float,
        latency_ms: int,
        similar: list[dict],        # [{id, score}] — SIMILAR_TO candidates
        informed_by: list[str],     # call ids whose context was actually injected
    ) -> None:
        async with self.driver.session() as session:
            await session.run(
                """
                MERGE (u:User {id: $user_id})
                MERGE (f:Feature {name: $feature_tag})
                MERGE (m:Model {name: $model})
                MERGE (p:Provider {name: $provider})
                CREATE (c:Call {
                    id: $call_id, prompt: $prompt, response: $response,
                    feature_tag: $feature_tag, tokens_in: $tokens_in,
                    tokens_out: $tokens_out, cost: $cost,
                    latency_ms: $latency_ms, created_at: datetime()
                })
                MERGE (u)-[:MADE]->(c)
                MERGE (c)-[:TAGGED]->(f)
                MERGE (c)-[:USED]->(m)
                MERGE (c)-[:ROUTED_TO]->(p)
                """,
                call_id=call_id,
                user_id=user_id,
                feature_tag=feature_tag,
                prompt=prompt,
                response=response,
                model=model,
                provider=provider,
                tokens_in=tokens_in,
                tokens_out=tokens_out,
                cost=cost,
                latency_ms=latency_ms,
            )
            if fallback_provider:
                await session.run(
                    """
                    MATCH (c:Call {id: $call_id})
                    MERGE (p:Provider {name: $fallback_provider})
                    MERGE (c)-[:FAILED_OVER_TO]->(p)
                    """,
                    call_id=call_id,
                    fallback_provider=fallback_provider,
                )
            if similar:
                await session.run(
                    """
                    MATCH (c:Call {id: $call_id})
                    UNWIND $similar AS s
                    MATCH (o:Call {id: s.id})
                    MERGE (c)-[r:SIMILAR_TO]->(o)
                    SET r.score = s.score
                    """,
                    call_id=call_id,
                    similar=similar,
                )
            if informed_by:
                await session.run(
                    """
                    MATCH (c:Call {id: $call_id})
                    UNWIND $informed_by AS oid
                    MATCH (o:Call {id: oid})
                    MERGE (c)-[:INFORMED_BY]->(o)
                    """,
                    call_id=call_id,
                    informed_by=informed_by,
                )

    async def write_cache_hit(
        self,
        *,
        call_id: str,
        user_id: str,
        feature_tag: str,
        prompt: str,
        cached_call_id: str,
        similarity: float,
    ) -> None:
        """A cache hit still becomes a node (audit trail), linked to the call
        whose response it was served from. No USED/ROUTED_TO — no model ran."""
        async with self.driver.session() as session:
            await session.run(
                """
                MERGE (u:User {id: $user_id})
                MERGE (f:Feature {name: $feature_tag})
                CREATE (c:Call {
                    id: $call_id, prompt: $prompt, feature_tag: $feature_tag,
                    cache_hit: true, created_at: datetime()
                })
                MERGE (u)-[:MADE]->(c)
                MERGE (c)-[:TAGGED]->(f)
                WITH c
                MATCH (o:Call {id: $cached_call_id})
                MERGE (c)-[r:SERVED_FROM_CACHE]->(o)
                SET r.score = $similarity
                """,
                call_id=call_id,
                user_id=user_id,
                feature_tag=feature_tag,
                prompt=prompt,
                cached_call_id=cached_call_id,
                similarity=similarity,
            )

    async def stats(self) -> dict:
        """Node/edge counts — handy for the demo and health checks."""
        async with self.driver.session() as session:
            result = await session.run(
                """
                MATCH (c:Call)
                OPTIONAL MATCH ()-[s:SIMILAR_TO]->()
                OPTIONAL MATCH ()-[i:INFORMED_BY]->()
                RETURN count(DISTINCT c) AS calls,
                       count(DISTINCT s) AS similar_edges,
                       count(DISTINCT i) AS informed_by_edges
                """
            )
            record = await result.single()
            return dict(record) if record else {}

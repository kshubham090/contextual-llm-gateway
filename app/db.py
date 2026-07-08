"""Postgres layer: raw call records, cost logs, and pgvector similarity search."""
import uuid
from datetime import datetime

import asyncpg

from .config import settings

SCHEMA = """
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS calls (
    id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    feature_tag TEXT NOT NULL,
    prompt TEXT NOT NULL,
    response TEXT,
    model TEXT,
    provider TEXT,
    tokens_in INT NOT NULL DEFAULT 0,
    tokens_out INT NOT NULL DEFAULT 0,
    cost NUMERIC(12, 6) NOT NULL DEFAULT 0,
    latency_ms INT NOT NULL DEFAULT 0,
    cache_hit BOOLEAN NOT NULL DEFAULT FALSE,
    fallback_used BOOLEAN NOT NULL DEFAULT FALSE,
    embedding vector({dim}),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS calls_embedding_idx
    ON calls USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS calls_user_day_idx
    ON calls (user_id, feature_tag, created_at);
"""


def _vec(embedding: list[float]) -> str:
    """asyncpg has no native pgvector codec — pass as text and cast in SQL."""
    return "[" + ",".join(f"{x:.8f}" for x in embedding) + "]"


class Database:
    def __init__(self) -> None:
        self.pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=10)
        async with self.pool.acquire() as conn:
            await conn.execute(SCHEMA.format(dim=settings.embedding_dim))

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()

    async def find_similar(
        self, embedding: list[float], limit: int, min_similarity: float
    ) -> list[dict]:
        """Nearest past calls by cosine similarity, best first.

        Only completed LLM calls (response present, not cache hits) are candidates —
        cache-hit rows carry no new information and would create duplicate matches.
        """
        rows = await self.pool.fetch(
            """
            SELECT id, prompt, response, feature_tag,
                   1 - (embedding <=> $1::vector) AS similarity
            FROM calls
            WHERE embedding IS NOT NULL
              AND response IS NOT NULL
              AND NOT cache_hit
            ORDER BY embedding <=> $1::vector
            LIMIT $2
            """,
            _vec(embedding),
            limit,
        )
        return [
            {
                "id": str(r["id"]),
                "prompt": r["prompt"],
                "response": r["response"],
                "feature_tag": r["feature_tag"],
                "similarity": float(r["similarity"]),
            }
            for r in rows
            if r["similarity"] >= min_similarity
        ]

    async def log_call(
        self,
        *,
        call_id: uuid.UUID,
        user_id: str,
        feature_tag: str,
        prompt: str,
        response: str | None,
        model: str | None,
        provider: str | None,
        tokens_in: int,
        tokens_out: int,
        cost: float,
        latency_ms: int,
        cache_hit: bool,
        fallback_used: bool,
        embedding: list[float] | None,
    ) -> None:
        await self.pool.execute(
            """
            INSERT INTO calls (id, user_id, feature_tag, prompt, response, model,
                               provider, tokens_in, tokens_out, cost, latency_ms,
                               cache_hit, fallback_used, embedding)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                    $14::vector)
            """,
            call_id,
            user_id,
            feature_tag,
            prompt,
            response,
            model,
            provider,
            tokens_in,
            tokens_out,
            cost,
            latency_ms,
            cache_hit,
            fallback_used,
            _vec(embedding) if embedding else None,
        )

    async def usage_rollup(
        self, user_id: str | None = None, feature_tag: str | None = None
    ) -> list[dict]:
        conditions, params = [], []
        if user_id:
            params.append(user_id)
            conditions.append(f"user_id = ${len(params)}")
        if feature_tag:
            params.append(feature_tag)
            conditions.append(f"feature_tag = ${len(params)}")
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        rows = await self.pool.fetch(
            f"""
            SELECT user_id, feature_tag, date_trunc('day', created_at) AS day,
                   count(*) AS calls,
                   count(*) FILTER (WHERE cache_hit) AS cache_hits,
                   coalesce(sum(tokens_in), 0) AS tokens_in,
                   coalesce(sum(tokens_out), 0) AS tokens_out,
                   coalesce(sum(cost), 0) AS cost,
                   coalesce(avg(latency_ms), 0) AS avg_latency_ms
            FROM calls
            {where}
            GROUP BY user_id, feature_tag, day
            ORDER BY day DESC, user_id, feature_tag
            """,
            *params,
        )
        return [
            {
                "user_id": r["user_id"],
                "feature_tag": r["feature_tag"],
                "day": r["day"].date().isoformat()
                if isinstance(r["day"], datetime)
                else str(r["day"]),
                "calls": r["calls"],
                "cache_hits": r["cache_hits"],
                "tokens_in": r["tokens_in"],
                "tokens_out": r["tokens_out"],
                "cost": float(r["cost"]),
                "avg_latency_ms": float(r["avg_latency_ms"]),
            }
            for r in rows
        ]

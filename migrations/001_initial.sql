-- Initial schema. Applied under an advisory lock; existing installations are preserved.
CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE IF NOT EXISTS calls (
    id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    feature_tag TEXT NOT NULL,
    prompt TEXT,
    response TEXT,
    model TEXT,
    provider TEXT,
    tokens_in INT NOT NULL DEFAULT 0,
    tokens_out INT NOT NULL DEFAULT 0,
    cost NUMERIC(16, 8) DEFAULT 0,
    latency_ms INT NOT NULL DEFAULT 0,
    cache_hit BOOLEAN NOT NULL DEFAULT FALSE,
    fallback_used BOOLEAN NOT NULL DEFAULT FALSE,
    embedding vector({dim}),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS calls_embedding_idx
    ON calls USING hnsw (embedding vector_cosine_ops);

-- Legacy memory cannot be attributed safely. Quarantine it instead of granting
-- a new tenant access. New application writes always supply tenant_id explicitly.
ALTER TABLE calls ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT '__legacy_quarantine__';
ALTER TABLE calls ADD COLUMN IF NOT EXISTS max_tokens INT;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS generation_config TEXT;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS embedding_space TEXT;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS cache_expires_at TIMESTAMPTZ;
ALTER TABLE calls ALTER COLUMN prompt DROP NOT NULL;
ALTER TABLE calls ALTER COLUMN cost DROP NOT NULL;
ALTER TABLE calls ALTER COLUMN cost TYPE NUMERIC(16, 8);
CREATE INDEX IF NOT EXISTS calls_scope_time_idx
    ON calls (tenant_id, user_id, feature_tag, created_at DESC);
CREATE INDEX IF NOT EXISTS calls_expiry_idx ON calls (expires_at)
    WHERE embedding IS NOT NULL;
CREATE TABLE IF NOT EXISTS graph_outbox (
    id BIGSERIAL PRIMARY KEY,
    call_id UUID NOT NULL UNIQUE REFERENCES calls(id) ON DELETE CASCADE,
    tenant_id TEXT NOT NULL,
    event JSONB NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until TIMESTAMPTZ,
    lease_token UUID,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS graph_outbox_available_idx
    ON graph_outbox (available_at, id);

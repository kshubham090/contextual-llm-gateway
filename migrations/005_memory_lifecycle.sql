-- PostgreSQL owns live memory state. Graph tombstones are a replay-safe projection.
CREATE TABLE IF NOT EXISTS memory_scopes (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    feature_tag TEXT NOT NULL,
    revision BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, user_id, feature_tag)
);
ALTER TABLE calls ADD COLUMN IF NOT EXISTS memory_revision BIGINT NOT NULL DEFAULT 1;
ALTER TABLE calls ADD COLUMN IF NOT EXISTS memory_status TEXT NOT NULL DEFAULT 'active';
ALTER TABLE calls ADD COLUMN IF NOT EXISTS memory_kind TEXT NOT NULL DEFAULT 'generated';
ALTER TABLE calls ADD COLUMN IF NOT EXISTS source_ids UUID[];
ALTER TABLE calls ADD COLUMN IF NOT EXISTS supersedes_id UUID REFERENCES calls(id);
ALTER TABLE calls ADD COLUMN IF NOT EXISTS retrieval JSONB NOT NULL DEFAULT '{}';
ALTER TABLE calls ADD COLUMN IF NOT EXISTS memory_visible BOOLEAN NOT NULL DEFAULT false;
UPDATE calls SET memory_visible = true WHERE prompt IS NOT NULL;
CREATE INDEX IF NOT EXISTS calls_memory_sources_idx ON calls USING gin(source_ids);
CREATE INDEX IF NOT EXISTS calls_memory_listing_idx
    ON calls (tenant_id, user_id, feature_tag, created_at DESC, id DESC)
    WHERE memory_visible;
-- A call can have an initial projection followed by correction/deletion events.
ALTER TABLE graph_outbox DROP CONSTRAINT IF EXISTS graph_outbox_call_id_key;
CREATE INDEX IF NOT EXISTS graph_outbox_call_idx ON graph_outbox(call_id);

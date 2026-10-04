-- Preserve graph dependencies: the oldest pending event in a memory scope is
-- projected first. Different tenants, users, and features can still run in parallel.
ALTER TABLE graph_outbox ADD COLUMN IF NOT EXISTS user_id TEXT;
ALTER TABLE graph_outbox ADD COLUMN IF NOT EXISTS feature_tag TEXT;
UPDATE graph_outbox SET user_id = coalesce(event->'payload'->>'user_id', '__invalid__')
    WHERE user_id IS NULL;
UPDATE graph_outbox SET feature_tag = coalesce(event->'payload'->>'feature_tag', '__invalid__')
    WHERE feature_tag IS NULL;
ALTER TABLE graph_outbox ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE graph_outbox ALTER COLUMN feature_tag SET NOT NULL;
CREATE INDEX IF NOT EXISTS graph_outbox_scope_order_idx
    ON graph_outbox (tenant_id, user_id, feature_tag, id);

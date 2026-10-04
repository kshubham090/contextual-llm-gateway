-- Exact completion lookup must not depend on approximate-neighbor recall.
-- The digest is an index accelerator only: queries also compare the full prompt.
-- Time-dependent TTL checks remain in the query, never in this partial index.
CREATE INDEX IF NOT EXISTS calls_exact_cache_idx
    ON calls (tenant_id, user_id, feature_tag, md5(prompt), created_at DESC)
    WHERE NOT cache_hit AND prompt IS NOT NULL
      AND response IS NOT NULL AND embedding IS NOT NULL;

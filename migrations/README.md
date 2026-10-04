# Database migrations

`Database.connect()` applies numbered SQL files in order, inside one transaction
under PostgreSQL advisory lock `827413509`. `gateway_schema_migrations` records
applied versions. Repeated starts and concurrent replicas are safe. Review and
apply migrations with a database role authorized to create the vector extension
and indexes before running a restricted application role in production.

`002` is additive: existing call IDs, costs, and embeddings are retained. Rows
created before tenant authentication are assigned `__legacy_quarantine__` and
have no embedding-space identity or retention expiry, so they cannot be returned
by memory retrieval. Never bulk reassign them to a tenant without verifying data
ownership. Neo4j uses new `Gateway*` labels so legacy graph nodes are likewise
excluded. Archive or erase that old data according to your retention policy.

The vector dimension is validated at startup. Changing embedding models of the
same dimension uses a new `embedding_space`; changing dimensions requires a
reviewed schema migration and re-embedding rather than an automatic destructive
conversion. The nearest-neighbor query orders directly by cosine distance so
pgvector can choose the HNSW index. Restrictive tenant filters may yield fewer
neighbors with approximate search; this must be evaluated on representative data.

`003` serializes pending graph events within each tenant/user/feature scope.
Different scopes still project concurrently. This preserves causal links when a
new call uses a just-written call whose graph event is still queued. A repeatedly
failing event blocks later events in its own scope and remains available for
operator inspection; it is never silently discarded.

`004` adds a scoped prompt-digest index for exact response reuse. Exact lookup
checks the complete prompt as well as tenant/user/feature, embedding space,
generation configuration, token budget, and both memory/cache TTLs. The digest is
only an accelerator, not a security boundary. This lookup does not depend on HNSW
recall; approximate retrieval retains its separate behavior described above.

Expired rows are excluded immediately. `purge_expired_memory()` removes expired
text, vectors, and queued graph payloads while preserving billing metadata;
`MemoryGraph.purge_expired_memory()` removes expired graph nodes and orphan
scope identifiers. The outbox worker runs one bounded maintenance batch every
minute (up to 1,000 rows/nodes); large expiry backlogs may require additional
controlled maintenance runs. Worker downtime delays physical deletion while TTL
filtering still excludes expired memory. Backups require a separate policy.

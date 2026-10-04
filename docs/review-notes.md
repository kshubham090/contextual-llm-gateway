# Adversarial review before merge

Three reviewers independently examined security/privacy, concurrency, and persistence/operations. Findings were challenged by another reviewer before fixes were accepted. This records code-review evidence, not a production certification or a formal human approval.

## Accepted findings and fixes

### Circuit state could be changed by an older request (P2)

A request admitted before a circuit trip could complete later and clear the newer cooldown. Cancelling an old attempt during a recovery probe could also release that probe's slot and admit a second probe. The concurrency reviewer reproduced both through the router API; the security reviewer independently reproduced and accepted the finding.

The router now records circuit generations and per-attempt probe ownership. Stale completions, errors, and cancellations cannot modify a newer generation; only the owning probe releases its slot. Five deterministic regressions fail on the earlier implementation and pass with the fix. See [provider regressions](../tests/test_providers.py).

### Exact response reuse depended on approximate-vector recall (P2)

An isolated PostgreSQL fixture with 10,001 rows naturally selected the HNSW index but missed an eligible exact prompt. An exact SQL scan found it; enabling iterative HNSW scanning alone did not resolve the reproduction. The security reviewer challenged its severity: this was avoidable generation cost and lost cache reliability, not a cross-tenant disclosure. It was accepted as a performance/correctness defect.

Exact reuse now uses an indexed scoped query before embedding or vector search. The digest only narrows candidates; full prompt equality, tenant/user/feature, generation policy, token budget, embedding namespace, and both TTLs are checked. Hits retain the same durable accounting and privacy behavior. The security reviewer independently accepted the query, index, and request path. Approximate recall remains a documented consideration for graph seeds and opt-in semantic caching.

See [database tests](../tests/test_db.py), [request-path tests](../tests/test_pipeline.py), [real-service regressions](../tests/test_services.py), and [migration 004](../migrations/004_exact_response_cache.sql).

## Concerns that were challenged and not filed as defects

- A graph probe with 500 calls and 11,424 relationships returned its bounded candidate set in 0.208 seconds on the review machine. This did not demonstrate the suspected traversal failure; larger graph capacity still requires measurement.
- Invalid-Unicode input was rejected by request validation before batching. A private upstream-error probe found no private prompt or credential in the HTTP response or application INFO logs and no durable content or embedding memoization.
- An arbitrary mocked batch rejection was insufficient evidence of a real accepted input that poisons other callers. Shared upstream outages remain possible; no unsupported security claim was made.
- Native CPU/GPU kernels cannot be forcibly cancelled by an asyncio timeout. The documented process/container shutdown boundary remains relevant; async cleanup itself did not demonstrate a new regression.

## Merge validation

The revised full suite passed locally: **158 tests**, including **nine PostgreSQL/pgvector, Neo4j, Redis, and full-HTTP integration tests**. Lint and whitespace checks passed. The real-service workflow now runs automatically on pull requests and main-branch updates. Final remote CI results are attached to the pull request for the exact revision being merged.

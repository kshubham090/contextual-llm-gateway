# 0.3.0 implementation and cross-review record

Three subagents implemented independent areas, then reviewed one another's changes. The root integrated the application, inspector, evaluation/load tools, and release documentation. The previous release's cache and circuit fixes remain covered by regression tests.

## Findings resolved before the PR

| Finding | Correction and evidence |
|---|---|
| Disconnect before response headers left first-token inference running | The prefetch phase now monitors disconnects and cancels/closes upstream before handing receive ownership to response streaming. The original ASGI reproduction and real SDK/HTTP SSE tests pass. |
| A derived answer could extend the life of an expired source | Finalization validates scoped source liveness with the database clock and caps memory/cache/graph expiry to the earliest source. A stale finalization retains content-free accounting and returns a conflict. Real PostgreSQL regression included. |
| An idempotent call-ID retry could bypass a changed scope revision | Replays cannot turn a stale/retired record into content-bearing success. Scope revision checks apply at finalization. |
| A timeline refresh failure mislabeled a committed stream as incomplete | A durable final event stays successful even if the subsequent memory-list request fails. Browser-handler regression included. |
| Draft content survived disconnect and could cross operator-selected scopes | Disconnect clears the token, prompt and edit drafts, record references, and rendered conversation. Browser-handler regression included. |
| Malformed HTTP 200 JSON could abort a load report | Response shape and numeric metadata are validated; malformed successes count as failures without losing the experiment. |
| Different actual models could confound a retrieval comparison | Complete and model-matched triplets are counted separately. A model/provider mismatch cannot enter the paired graph-versus-vector metric. |
| Package verification and demo upgrade instructions were inconsistent | CI packs from the TypeScript package directory; switching a demo to real inference preserves the existing environment and database passwords. |
| SDK status types and reproduction settings lagged the final API/run | Both client types include expired memory, and the local-model instructions specify the actual run's frozen graph threshold. |
| An old evaluation request could cancel its successor's native job | Cancellation now signals a request-local stop flag. The reviewer's deterministic completion-delivery race is covered by a regression. |

## Console completion and additional cross-review

The operator screen now exposes overview, playground, live graph, library, comparison and connection views. The local launcher starts the HTTP application instead of requiring users to open an HTML source file. The normal gateway was verified with real pinned Qwen generation, local MiniLM embeddings and the three backing stores; the synthetic demo remains clearly separate.

- Added scoped accounting and graph data endpoints, including bounds, live PostgreSQL rehydration after graph reads, mutation revision checks, outage handling and safe configuration metadata.
- Fixed late detail selections, reconnect callbacks and post-mutation refreshes crossing browser sessions. Stale responses and errors cannot overwrite a newer workspace; mutation invalidation clears pending reads and exports.
- Fixed empty model selector values and empty-cohort latency display. Token-limit/filter stops remain visible alongside durable completion. Comparisons show failed/unrun modes and model/revision mismatches explicitly.
- Verified 26 launcher tests, including exact env-file selection, occupied-port refusal and owned-process cleanup on interrupt/termination. Backing-service volumes are preserved.
- Browser verification exercised actual local-model chat, source inspection, real graph relationships, three retrieval modes and inspectable/copied JSON exports. Export text excludes the active credential.

## Local verification

- 288 gateway/tool tests, including 28 opted-in PostgreSQL/Neo4j/Redis service tests.
- 26 Python client tests, 21 TypeScript client tests, and 25 console behavior tests: 360 in total.
- Python lint, JavaScript syntax, client package builds, and whitespace validation.
- Both support client examples exercised creation, correction, streaming final metadata, listing, and scope deletion over real loopback HTTP.
- Browser workflow verified at desktop and 390-pixel mobile widths: load synthetic records, stream an answer, follow a source, correct the record, and retrieve the replacement. Preview images contain fictional data and no token.

The provider protocol tests use actual Anthropic SDK/httpx parsing with mocked transports and gated byte streams. They prove wire/cancellation behavior without paid requests. Demo generation is explicitly synthetic. A separate pinned local-model server has 21 contract tests and is used for actual-generation evaluation. No hosted-model answer-quality result, NVIDIA CUDA measurement, or real-world capacity guarantee is claimed. CPU/MPS HTTP measurements and their limitations are documented separately in [end-to-end results](end-to-end-results.md).

The PR's CI checks are the authority for the pushed revision. Package registry publication, deployment and PR merging are separate from this source release.

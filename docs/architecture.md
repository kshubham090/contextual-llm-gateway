# Architecture and invariants

The gateway's unit of memory is a past interaction. Its retrieval boundary is `(tenant_id, user_id, feature_tag)`. Authentication derives the tenant; a trusted calling application supplies the other identifiers. This design deliberately trades team-wide sharing for a conservative default. A future sharing feature needs explicit grants and tests, not a relaxed database filter.

## Request path

1. Assign a request ID, bound incoming body size and concurrent intake buffers, authenticate the bearer key, then enforce request admission, time, and Redis rate limits.
2. If caching is enabled, look up the exact prompt with a scoped digest index and full-text equality check. Enforce both TTLs, embedding namespace, generation configuration, and token budget. An eligible hit proceeds directly to durable accounting; it does not depend on embedding availability or approximate-neighbor recall.
3. On an exact miss, embed and query scoped vector neighbors only when graph context, new stored memory, or explicit semantic caching needs them. Bounded workers coalesce embeddings; private calls bypass embedding memoization. `use_graph: false` disables context injection; a stored call still builds graph links. Semantic reuse still requires matching scope, namespace, generation configuration, token budget, and TTLs.
4. Use vector neighbors as seeds for a bounded Neo4j neighborhood. Rank candidates by vector similarity, exponentially decaying recency, and feature affinity, then enforce snippet and total context budgets. Feature affinity is constant under current strict feature isolation; the term is retained for ranking compatibility.
5. Treat prior exchanges as untrusted evidence. Delimit and escape them before supplying context. Route generation with bounded provider concurrency, timeouts, retryable-error fallback, and per-model circuit state.
6. Commit the accounting row and graph event in a PostgreSQL transaction before returning success. With `store: false`, write no prompt, response, vector, or graph event. Return answer metadata and stage durations.
7. A separate in-process outbox loop claims leased events, writes idempotently to Neo4j, and acknowledges completion. Multiple replicas can compete for leases. Events from the same scope preserve ordering.

Neo4j is required during startup to establish schema and constraints. If graph retrieval fails after startup, scoped vector matches may still supply context. Such degradation is exposed in response metadata. When durable accounting fails, a generated answer can have incurred a provider charge even if the client receives an error. Clients should not blindly retry writes: there is no public idempotency-key contract.

## Stores and consistency

| Component | Authoritative responsibility | Consistency |
|---|---|---|
| PostgreSQL | Billing rows, source memory, vector search, pending graph events | Call and graph event commit atomically. |
| Neo4j | Relationships and bounded contextual traversal | Eventually consistent projection. |
| Redis | Tenant and user request windows | Rate limits depend on shared Redis availability. |
| Process memory | Admission state, circuits, embedding queue/cache | Per replica; reset on restart. |
| LLM and embedding provider | Generation and remote embeddings | External dependency with separate data and billing policies. |

Graph edges record relationships: `SIMILAR_TO` links semantically related calls, `INFORMED_BY` records supplied context, and `SERVED_FROM_CACHE` records response reuse. An `INFORMED_BY` edge does not establish that every linked statement influenced the output or that the output is true.

## Bounded work

`MAX_CONCURRENT_REQUESTS` limits admitted requests. Request bodies also pass a bounded intake gate before authentication, limiting concurrent body buffers to twice that value. `EMBEDDING_QUEUE_SIZE`, batch size, workers, and cache size bound embedding work. A separate aggregate UTF-8 byte budget splits batches; an individual long input still follows the selected embedding model's own context or truncation rules. Provider concurrency and queue timeouts bound generation requests. Circuit generations prevent late completions from clearing a newer cooldown; only the owning recovery probe can release its slot. Context count and character budgets bound input expansion. The outbox batch and worker concurrency bound replay work. Connection pools must be sized across all replicas, not independently against the database's total connection budget.

The HNSW index accelerates approximate nearest-neighbor search; strict scope filtering can reduce recall for graph seeds and opt-in semantic caching. Exact completion reuse uses a separate deterministic query and is unaffected by ANN recall. Validate plans and recall on representative tenant sizes. Local GPU inference is optional and has no automatic speed guarantee. Ranking and traversal still consume CPU and database work.

## Failure boundaries

A graceful shutdown stops intake, bounds pending work, and leaves unacknowledged events durable for replay. An abrupt process termination cannot erase an already committed event. A graph outage can accumulate an outbox backlog, so disk usage and oldest pending-event age need alerts. Repeatedly failing events require operator investigation; do not claim delivery merely because an HTTP response succeeded.

The gateway has no tool execution capability. Prompt injection can still corrupt an answer or future memory within an authorized scope. Text escaping is defense in depth, not a semantic security boundary. Retention TTL filters stop retrieval at expiry; physical cleanup and backup expiry are separate responsibilities.

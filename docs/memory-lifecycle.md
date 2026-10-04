# Memory lifecycle and provenance

Memory can be inspected, corrected, and removed through authenticated APIs. PostgreSQL
is the authority for live content and revisions. Neo4j supplies relationships; a
projected graph node is never sufficient authorization to reuse its text.

## Ownership and scope

A gateway bearer key identifies a trusted application tenant. Every memory operation
also requires an exact `user_id` and `feature_tag`. The application is responsible for
authorizing those two identifiers for its own end users. These are application keys,
not end-user sessions. A tenant field in a mutation body is rejected.

Listing, detail, correction, deletion, vector retrieval, and graph hydration all
apply the same `(tenant_id, user_id, feature_tag)` boundary. An unknown ID and an ID
outside that boundary both return 404. There is no implicit sharing between users or
features.

## API

All endpoints use `Authorization: Bearer <gateway token>`. IDs are UUIDs. Responses
are JSON. The gateway applies its user/tenant rate limits to memory operations as
well as chat; callers should respect 429 and `Retry-After`.

| Method and path | Input | Result |
| --- | --- | --- |
| `GET /v1/memories` | Required query `user_id`, `feature_tag`; optional `limit` 1–100 and `cursor` | `items`, `next_cursor`, `scope_revision` |
| `GET /v1/memories/{id}` | Required query `user_id`, `feature_tag` | One memory, including its revision and provenance |
| `POST /v1/memories` | JSON `user_id`, `feature_tag`, `prompt`, `response`, `expected_scope_revision` | New curated memory; HTTP 201 |
| `PATCH /v1/memories/{id}` | JSON `user_id`, `feature_tag`, `prompt`, `response`, `expected_revision` | New replacement memory and updated scope revision |
| `POST /v1/memories/{id}/corrections` | Same JSON as PATCH | Alias for correction; HTTP 201 |
| `DELETE /v1/memories/{id}` | Query `user_id`, `feature_tag`, `expected_revision` | Deletion and dependent-memory invalidation summary |
| `DELETE /v1/memories` | Query `user_id`, `feature_tag`, `expected_scope_revision` | Removes all active retained content in that exact scope |

Read the collection first to obtain its `scope_revision`; a previously unused scope
has revision zero. Read a memory before correcting or deleting it. A stale expected
revision returns 409 without overwriting newer work. Refresh and review the current
state before retrying. Invalid input returns 422; a provider or storage failure does
not return raw backend error details.

Prompts and responses in curated creation/correction must each contain nonblank text,
have no null bytes, and fit 64,000 characters. The aggregate HTTP body limit still
applies. These operations embed the prompt, then check the revision again inside the
commit transaction. They never call a generation model. Curated entries can become
context and receive scoped `SIMILAR_TO` links, but do not claim `INFORMED_BY` provenance
and are excluded from generation usage counts.

Creation and correction return:

```json
{
  "memory": {"id": "...", "revision": 1, "status": "active"},
  "scope_revision": 2,
  "invalidated_count": 0,
  "graph_write": "queued"
}
```

The full memory object also includes `user_id`, `feature_tag`, `prompt`, `response`,
`memory_kind` (`generated` or `curated`), `created_at`, `expires_at`, `source_ids`,
`supersedes_id`, `cache_eligible`, and `retrieval`. Generated records expose source IDs
and, for new requests, retrieval mode, similarity, ranking score, and selection reason.
A `graph_neighbor` reason means traversal proposed the source; it does not prove the
model used every fact in that source. Provider responses can still be incorrect.

Deletion results include `deleted_count`, `deleted_ids`, `deleted_ids_truncated`,
`invalidated_count`, `scope_revision`, and `graph_write`. At most 1,000 affected IDs are
returned; `deleted_count` covers the complete committed operation. For an individual
mutation, `invalidated_count` counts dependent or legacy records beyond the selected
record. For a scope deletion it counts all affected records.

## Browsing and pagination

The inspector retains content-free records with status `deleted`, `superseded`, or
`invalidated`. Expired content is returned as status `expired` with null text even if
background cleanup has not yet run. Private chat accounting rows are not memory
entries and do not appear in this listing.

Pagination uses a descending `(created_at, id)` key and a scope-bound cursor, rather
than offsets. Newer records do not shift an existing cursor's position. Each page
reads a consistent database snapshot. A scope mutation invalidates outstanding
cursors with 409; restart the listing. Cursors are pagination positions, not access
tokens, and every page still requires authentication and the full scope.

## Correction and invalidation

A correction creates a new ID with `supersedes_id` pointing to the selected memory.
The selected record becomes a content-free `superseded` record with an incremented
revision. It is not edited in place.

Deletion and correction clear the selected prompt, response, and vector, and remove
its old queued graph payload. They also recursively invalidate generated records
whose `source_ids` refer to the selected record or another invalidated descendant.
This includes stored cache-hit aliases. Independently authored evidence remains
available. Similarity edges alone are not treated as dependency edges.

Pre-lifecycle generated records have unknown provenance (`source_ids IS NULL`). A
mutation conservatively retires such records within the same scope, because the
service cannot prove that their answers are independent. New requests record an
explicit empty source list or their actual source IDs. Review this behavior before
upgrading a populated pre-lifecycle deployment.

Every curated creation, correction, or deletion increments a PostgreSQL scope revision
and invalidates completion-cache eligibility in that scope. New generation cache keys
and embedding namespaces include that revision. This prevents reuse of a completion
or embedding memoization entry from an earlier revision, including on another replica.

The application does not interpret semantic similarity as proof that two independent
facts contradict one another. Use correction to replace known bad evidence. Content
explicitly supplied again in a new request or its history is fresh caller input, not
an authorized-memory lookup.

## In-flight requests

A chat reads the scope revision before retrieval. Graph candidates are hydrated from
current PostgreSQL rows before becoming context. When finalizing either a generated
answer or a cache hit, the service locks the scope row and verifies that the revision
has not changed. The check and persistence share one transaction. Source IDs are also revalidated in
that transaction against their scope, active state, and current expiry. A generated
record and its graph event cannot outlive the earliest source expiry; deriving another
answer does not renew a source fact's retention lifetime.

If a mutation won the race or a source expired before finalization, the service commits
only content-free accounting and
raises a memory conflict. No stale prompt, answer, vector, or graph event is written.
The regular API returns 409. A stream that already emitted provisional text reports
a nondurable error instead of a durable final event; clients cannot retract bytes
already displayed. A provider may already have charged, so the accounting metadata
is retained. A final response that committed before a later mutation is ordered
before that mutation.

## Graph replay and erasure limits

`graph_write: "queued"` means the PostgreSQL transaction committed. It does not mean
Neo4j has already removed its copy. Graph events remain ordered within the scope.
Deletion events create content-free, revisioned graph tombstones and remove the old
relationships. Writes acquire the same node lock and check the revision before
projecting content. A worker holding an old leased payload cannot recreate deleted
text even if it finishes after the deletion event or replays following a crash.

Tombstones retain the call ID, scope identifiers, key, revision, and deleted state.
They deliberately do not expire: removing a fence without proving old events cannot
replay would permit resurrection. Orphan graph scope/model/provider identifiers are
cleaned by routine maintenance. PostgreSQL accounting and content-free provenance
remain after a memory mutation.

Live PostgreSQL rows and pending outbox payloads lose affected content in the mutation
transaction. Neo4j cleanup is asynchronous and can lag during an outage. In-process
embedding caches contain hashed keys and vectors rather than prompt text; an old
scope namespace is no longer reusable, but its allocated entries can remain until
cache eviction or process shutdown. TTL expiry prevents reuse, not a promise of
immediate memory zeroing.

This API does not erase database backups, snapshots, WAL, externally managed replicas,
provider records, application logs outside the gateway, or text already delivered to
a client. Database MVCC and storage reclamation also differ from overwriting physical
disk bytes. Coordinate those retention policies separately. Providers still process
submitted text according to their own terms. Do not describe this API as a universal
physical-erasure guarantee.

## Validation

`tests/test_memory_services.py` runs against real PostgreSQL and Neo4j in an isolated
schema and unique graph tenant. It exercises revisions, scoped listing and pagination,
correction and transitive invalidation, content-free conflict accounting, queued and
stale leased graph events, concurrent graph deletion/write fencing, curated graph
links, cache invalidation, expiry inspection, and authenticated HTTP operations.
Use `RUN_SERVICE_TESTS=1` only with an appropriate test backing-service configuration.

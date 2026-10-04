# Operations and release checklist

The Compose stack is a development environment. A production deployment needs a trusted TLS ingress, private backing-service networks, managed secrets, monitored storage, backups with tested restores, and a measured capacity plan. The application should run as one non-root process per container; replicate containers for scale.

## Bring-up and health

Fill in `.env` from the example using separate random secrets, then start the stack. Database passwords in the supplied URL templates must be URL-safe or percent-encoded; generated hex values avoid ambiguity.

```bash
docker compose up --build -d
docker compose ps
curl --fail http://127.0.0.1:8000/health/live
curl --fail http://127.0.0.1:8000/health/ready
```

`/health/live` indicates the process can respond. `/health/ready` checks essential dependencies and worker health. Neo4j must be available during startup for schema and constraints. A later graph outage can report degraded readiness while requests continue with scoped vector fallback; inspect the response status details. Do not use liveness as evidence that generation providers are functioning. Model access, quotas, and region availability require an actual authorized smoke request.

`ENVIRONMENT=production` refuses `AUTH_ENABLED=false`. Empty provider credentials or bearer-key configuration are rejected during service startup. Keep metrics credentials distinct from tenant application tokens. The image uses a non-root account, a healthcheck, and bounded shutdown; Compose adds a read-only root, temporary filesystem, dropped capabilities, and resource limits.

## Migration and model changes

Read [`migrations/README.md`](../migrations/README.md) and review every SQL file before upgrading a populated database. Startup applies ordered migrations under a PostgreSQL advisory transaction lock and records applied versions. Large index or schema changes can hold locks and consume storage; test duration against a restored production-sized copy and plan the maintenance window.

Legacy rows with no authenticated owner are assigned `__legacy_quarantine__`. New graph labels exclude old unscoped nodes. Do not bulk assign old memory to an active tenant without verifying ownership. Quarantine does not delete old content or backups.

Vector dimensions are checked at startup. To change dimensions, back up and test a reviewed schema migration/re-embedding process, or create a new deployment/database. Do not overwrite an existing vector collection and hope the values remain compatible. Models of the same dimension still use separate embedding namespaces. Set `LOCAL_EMBEDDING_REVISION` to a tested immutable model commit for local inference. The revision participates in embedding namespace identity. A newer model revision requires its own evaluation and a controlled model artifact.

The current application applies migrations at connect time. Provision a deployment role with the privileges needed for extension/index/schema changes; validate a restricted runtime-role rollout against your PostgreSQL configuration. A separate migration-only deployment command is not currently supplied.

## Durable graph replay

The response follows the PostgreSQL transaction that stores billing and the graph event. Neo4j indexing is eventual. `meta.memory_write="queued"` means the event committed, not that graph relationships are already visible.

Workers claim bounded batches with leases and acknowledge only after idempotent graph writes. Same-scope ordering preserves dependencies. Failed events are retried with bounded delay; a poison event can delay later work in that scope. Monitor queue depth, event age, attempts, and bounded error categories:

```sql
SELECT count(*) AS pending,
       min(created_at) AS oldest_pending,
       max(attempts) AS maximum_attempts
FROM graph_outbox;
```

If Neo4j is unavailable, restore it and allow durable events to drain. Check worker logs and scoped graph counts. Do not manually acknowledge or delete pending events merely to clear an alert. Diagnose malformed events against the matching schema/version; preserve a backup before any corrective mutation. A process crash after a graph write but before acknowledgment causes safe replay, not a new caller-visible generation.

## Retention and deletion

The default memory TTL is thirty days; completion-cache TTL is one hour. Expired memories are excluded from retrieval immediately. The outbox worker runs bounded expiry cleanup approximately once per minute for PostgreSQL memory, queued graph payloads, and Neo4j nodes. Cleanup may lag during outages, backlog, or large expirations. Verify physical deletion when required; a TTL is not an exact erasure deadline.

Accounting remains after content cleanup. Define a separate accounting and identifier retention policy. Remove quarantined legacy content and backups according to your policy. A complete per-person deletion workflow, legal hold support, and backup erasure automation are not included.

## Observe and establish objectives

```bash
curl --fail http://127.0.0.1:8000/metrics \
  -H "Authorization: Bearer $METRICS_BEARER_TOKEN"
```

Scrape each application replica. Metrics omit raw prompts and high-cardinality user/tenant labels. Request IDs connect HTTP logs with pipeline events. Sensitive upstream payloads should not be copied into logs.

| Metric | Interpretation |
|---|---|
| `gateway_http_requests_total` | HTTP response counts by bounded route and status labels. |
| `gateway_http_duration_seconds` | HTTP duration histogram by route. |
| `gateway_stage_duration_seconds` | Stage histograms for locating request-path overhead. |
| `gateway_active_requests` | Requests occupying application admission slots. |
| `gateway_cache_total` | Completion-cache outcomes. |
| `gateway_degraded_total` | Requests with degraded graph retrieval. |
| `gateway_graph_outbox_pending` | Pending events from the worker's latest backlog snapshot. |
| `gateway_graph_outbox_oldest_seconds` | Age of the oldest pending event in that snapshot. |
| `gateway_graph_outbox_failed` | Pending events with failed attempts, not a cumulative failure counter. |
| `gateway_embedding_queue_depth` | Embedding items awaiting a batch. |
| `gateway_embedding_cache_entries` | Resident in-process embedding-cache entries. |

Backlog snapshots refresh approximately every five seconds while the worker runs. An initial or stale zero does not establish recovery; use readiness, logs, and the backing-service query together. Instance counters reset when the process restarts.

Define availability as successful eligible `/v1/chat` requests divided by eligible requests over a stated window. Decide explicitly how client validation errors, quota rejections, and user cancellations are classified. Set separate latency objectives for exact-cache hits and uncached provider generation after a representative baseline. Track quality regressions alongside latency; a fast answer with empty or incorrect context is not success.

Example candidate objectives to validate, not measured guarantees: a 99.9% monthly service-availability objective and a sixty-second normal-load graph-visibility objective. Establish latency targets from your selected model, prompt/token mix, and region. Alert on sustained error-budget burn, outbox age, repeated circuit opening, admission rejections, resource pressure, and retrieval quality drift. Do not copy example targets into an SLO without measuring feasibility.

## Recovery

| Symptom | First checks | Recovery direction |
|---|---|---|
| Startup fails | Required secrets, vector dimension, migration privileges, dependency reachability | Correct configuration; restore/test migration before retrying schema changes. |
| Repeated 429 | Tenant/user windows and provider quota; `Retry-After` | Apply bounded client backoff and inspect workload fairness. |
| 503 or deadlines | Admission/queue pressure, Redis, database, provider circuits | Reduce intake, restore dependency, then scale within downstream capacity. |
| Sparse context | Correct scope, thresholds, TTL, embedding namespace, graph lag | Check known seeds and retrieval recall before reducing isolation. |
| Growing outbox | Neo4j health, oldest event, attempts, worker task | Restore graph capacity and replay; investigate a blocking event. |
| Unexpected spend | Uncached calls, context tokens, retries, unknown pricing | Compare usage to provider billing; estimates exclude embeddings and infrastructure. |

Back up PostgreSQL and Neo4j with compatible checkpoints and record the schema/model version. Restore into an isolated environment and verify scoped retrieval and outbox replay before routing traffic. PostgreSQL holds the source calls and outstanding events, but an arbitrary Neo4j restore can lose already-acknowledged relationships; full graph reconstruction tooling is not supplied. A PostgreSQL-only backup is not a complete point-in-time graph recovery plan.

Use rolling deployments only after checking schema compatibility and total per-replica pool limits. Stop accepting new requests before shutdown. A response lost after a successful provider call may still be billed; the public API has no idempotency-key guarantee. Coordinate retries with the calling application.

## Release validation

Run lint, unit tests, fixture validation, real service integration, dependency auditing, and an image build. The real-service suite runs automatically for pull requests and main-branch updates. Execute a real provider smoke request and selected held-out scenarios in staging. Inspect both successful and failure-path reports. Test a backing-service interruption, graceful termination, restore, and sustained load before promoting the image. The image installs `requirements.lock`, the exact runtime package versions exported from the tested Linux Python 3.12 image. Pin the base image digest and retain approved package artifacts in the release system; package-version pins alone do not produce byte-for-byte identical images.

## Updating the runtime lock

`requirements.txt` contains human-maintained direct dependency bounds. `requirements.lock` snapshots the exact resolved runtime packages used by the application image. Development and optional acceleration installs include this core lock. Optional Torch/Sentence Transformers dependencies remain separately selected because CPU, CUDA, and MPS builds differ; record and validate their resolved versions for each accelerator image.

To propose an updated runtime snapshot, resolve the bounded source in a clean Python 3.12 container without mounting secrets:

```bash
docker run --rm -v "$PWD:/src:ro" python:3.12-slim sh -c \
  'python -m pip install --no-cache-dir -r /src/requirements.txt >&2 && python -m pip freeze' \
  > requirements.lock.candidate
```

Review the package changes and preserve the `uvloop` platform marker excluding Windows and non-CPython interpreters. Update the lock's date, Python version, and source platform. Replace the lock only after review, then run the dependency audit, unit tests, backing-service integration tests, and a rebuilt image smoke test. Remove the candidate after incorporating it. Record the new image digest with the release.

For an already tested image, `docker run --rm --entrypoint python <tested-image> -m pip freeze` exports its installed package snapshot. This is how the initial lock was produced. The lock does not contain hashes or GPU wheels and should not be described as a universal cross-platform artifact lock.

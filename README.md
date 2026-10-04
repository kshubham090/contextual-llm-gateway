<div align="center">

# Lowq X2 · Contextual LLM Gateway

**Give repeated LLM requests scoped memory, traceable context, and measurable operating limits.**

[![CI](https://github.com/kshubham090/contextual-llm-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/kshubham090/contextual-llm-gateway/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.12%2B-3776AB)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/API-FastAPI-009688)](https://fastapi.tiangolo.com/)
[![Memory](https://img.shields.io/badge/Memory-pgvector%20%2B%20Neo4j-4581C3)](docs/architecture.md)

[Quickstart](#quickstart) · [Architecture](docs/architecture.md) · [Use cases](docs/use-cases.md) · [Evaluation](docs/evaluation.md) · [Operations](docs/operations.md) · [Security](docs/security.md)

</div>

An LLM already knows general concepts. It usually does not know **your service's rollback caveat, your experiment's calibration rule, or your customer's previous connector issue**. This gateway retrieves related past interactions from a scoped graph and supplies them as context to the next request.

The core idea is unchanged: **traffic becomes retrievable memory**. PostgreSQL with pgvector finds related calls; Neo4j connects their neighborhoods; a bounded ranking stage selects context; an LLM answers; a durable event records new memory. Response metadata identifies the exact calls supplied to the model. These identifiers establish context provenance, not proof that an answer is correct.

This repository provides a hardened, testable foundation with measured local embedding improvements. Production readiness depends on the deployment, data policy, workload, and provider behavior. The measurements below cover warm local inference; end-to-end gateway performance and answer quality require separate evaluation.

## What is implemented

| Concern | Behavior |
|---|---|
| Identity and memory | Bearer keys map to tenants. Cache reads, vector retrieval, and graph traversal are bounded by tenant, user, and feature. |
| Cache correctness | An indexed exact-prompt lookup runs before embedding, with generation configuration, token budget, and TTL checks. Semantic reuse requires an explicit request flag. |
| Durable memory | Billing and a graph event commit in one PostgreSQL transaction before success. A leased outbox worker projects events into Neo4j. |
| Failure control | Request limits, admission bounds, provider concurrency limits, timeouts, model fallback, and circuit breaking. |
| Efficient inference | Bounded embedding batches, reusable clients, a scoped TTL embedding cache, and optional local CPU/CUDA/MPS inference. |
| Privacy control | `store: false` keeps prompt, response, embedding, and graph payload out of persistent memory and the embedding cache; content-free accounting remains. |
| Measurement | Request IDs, stage timing metadata, health/readiness probes, Prometheus metrics, synthetic evaluation fixtures, and a reproducible embedding benchmark. |

## Architecture

```mermaid
flowchart LR
    C[Trusted application] --> A[Authenticate tenant<br/>admit and rate limit]
    A --> X{Scoped exact cache hit?}
    X -->|yes| T[Commit accounting<br/>+ optional graph event]
    X -->|no| E[Embed when needed<br/>in bounded batches]
    E --> P[(PostgreSQL + pgvector)]
    P --> K{Opt-in semantic hit?}
    K -->|yes| T
    K -->|no| G[(Scoped Neo4j neighborhood)]
    G --> B[Rank and budget<br/>untrusted context]
    B --> L[LLM router<br/>bounded fallback]
    L --> T
    T --> R[Answer + provenance + timings]
    T --> O[(Durable outbox)]
    O --> W[Leased replay worker]
    W --> G
```

Graph memory can improve continuity when past interactions contain useful facts. It also adds retrieval work, prompt tokens, and potential stale or incorrect context. Measure those tradeoffs on the intended workload. For general one-off questions, use `use_graph: false`.

## Quickstart

Requires Python 3.12+ for local scripts, Docker Compose v2, an Anthropic API key, and a Voyage API key for the default embedding backend. Provider calls cost money. The fixture validator and unit tests do not need keys or running services.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
python scripts/evaluate.py validate
pytest -q
```

To start the gateway:

```bash
cp .env.example .env
python -c 'import secrets; print(secrets.token_hex(24))'
```

Generate separate random values for the gateway token, metrics token, and each infrastructure password. Fill in `.env`, including `ANTHROPIC_API_KEY`, `VOYAGE_API_KEY`, `POSTGRES_PASSWORD`, `NEO4J_PASSWORD`, and `REDIS_PASSWORD`. Set the gateway mapping to a JSON object such as `GATEWAY_API_KEYS={"<your-random-token>":"demo-team"}`. An empty mapping is rejected at startup.

```bash
docker compose up --build -d
curl --fail http://127.0.0.1:8000/health/ready
```

The API is at [localhost:8000](http://127.0.0.1:8000/docs); the Neo4j browser is at [localhost:7474](http://127.0.0.1:7474). Compose exposes ports on loopback only. It is a local development stack, not a public deployment template.

Set the client token to the same secret used as a key in `GATEWAY_API_KEYS`:

```bash
export GATEWAY_API_KEY='<your-random-token>'
curl --fail-with-body http://127.0.0.1:8000/v1/chat \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"Our Orbit rollback restores the image but requires a separate config revert. Summarize that rule.","user_id":"engineer-42","feature_tag":"deployments","max_tokens":250}'
```

For a host-run app, start `docker compose up -d postgres neo4j redis`, set the host connection URLs in `.env` to the matching passwords, and run `uvicorn app.main:app --reload`.

## API contract

`POST /v1/chat` accepts:

```json
{
  "prompt": "What should I do when an Orbit release and its configuration both need rollback?",
  "user_id": "engineer-42",
  "feature_tag": "deployments",
  "max_tokens": 500,
  "use_graph": true,
  "use_cache": true,
  "cache_mode": "exact",
  "store": true
}
```

| Option | Meaning |
|---|---|
| `use_graph` | Supply selected scoped memory as context. Stored calls still build memory links when this is false. |
| `use_cache` | Permit completion reuse when cache eligibility checks pass. |
| `cache_mode` | `exact` by default. `semantic` allows approximate prompt matches and can change answer suitability. |
| `store` | Persist reusable memory when true; retain only content-free accounting when false. Memory reads remain available. |

The response contains `response` and `meta`, including `call_id`, `context_used`, `cache_hit`, `cached_call_id`, `model`, `fallback_used`, `tokens_in`, `tokens_out`, `cost`, `cost_is_estimate`, `timings_ms`, `degraded`, and `memory_write`. A `queued` memory write means PostgreSQL committed the graph event; Neo4j visibility follows asynchronously. Cost is a generation estimate; embedding charges and infrastructure are excluded. Unknown model pricing is represented as `null` and counted as unpriced usage.

`GET /v1/usage` returns the authenticated tenant's accounting grouped by user, feature, and day. `GET /v1/graph/stats` returns scoped memory counts. Both accept optional `user_id` and `feature_tag` filters. `/health/live` and `/health/ready` support probes; `/metrics` requires its separate metrics bearer token.

**Trust boundary:** a gateway bearer key belongs to a trusted application that supplies the `user_id`. Do not distribute a tenant credential to untrusted end users. [Security details](docs/security.md) explain the boundary, retention, and remaining responsibilities.

## Six use cases with held-out questions

Each pack contains four synthetic memories, two questions absent from the seeds, expected fact aliases, and forbidden fact probes. Memory scopes are disjoint.

| Pack | What the held-out questions test |
|---|---|
| [Orbit deployment incidents](docs/use-cases.md#orbit-deployment-incidents) | Connect image rollback, configuration reversal, rollout gates, and audit references. |
| [Cedar support continuity](docs/use-cases.md#cedar-support-continuity) | Recover a connector workaround and the escalation evidence. |
| [Lantern experiment protocols](docs/use-cases.md#lantern-experiment-protocols) | Recall calibration, randomization, drift thresholds, and review ownership. |
| [Quartz data migrations](docs/use-cases.md#quartz-data-migrations) | Reconstruct a dual-write window, per-tenant checksums, and rollback constraints. |
| [Harbor maintenance training](docs/use-cases.md#harbor-maintenance-training) | Retrieve a fictional rig's qualified review and return-to-service checklist. |
| [Meadow preview releases](docs/use-cases.md#meadow-preview-releases) | Recall synthetic-data requirements, expiry, access, and review responsibilities. |

Run a single comparison against the running gateway:

```bash
python scripts/evaluate.py run --scenario orbit-incident --output artifacts/orbit.json
```

This seeds a fresh scope, waits for graph indexing, alternates graph-off/on calls with caching disabled, and checks unseeded user and feature controls. It saves responses, provenance, timings, costs, fixture hash, and lexical fact scores. An all-pack run makes 60 generation requests, including seed and isolation-control calls. Provider retries can add requests.

For a narrated two-step demo:

```bash
python scripts/seed_demo.py --run-id walkthrough
python scripts/demo_compare.py --run-id walkthrough
```

The scorer is a transparent lexical quality proxy, not an expert correctness or safety score. Paid-provider graph-answer quality has not been measured in the bundled reports. [Evaluation guide](docs/evaluation.md).

## Acceleration with evidence

Real MiniLM inference on an Apple M5, measured on 2026-10-04:

| Same-device comparison | Median throughput ratio, batch 32 versus batch 1 | Median batched throughput |
|---|---:|---:|
| CPU, 4 native threads | **4.998×** | 1,502.88 texts/s |
| Apple MPS | **8.360×** | 2,207.08 texts/s |

Three trials per device; 512 short synthetic texts per mode; concurrency 64; fixed model revision; memoization disabled. Loading and warmup are excluded. Ratios are medians of paired trial ratios. These measurements cover local embeddings, not hosted LLM generation or whole-gateway throughput. [Methodology and raw reports](docs/performance-results.md).

The default Voyage backend coalesces requests into bounded batches. Local embeddings can run with Sentence Transformers on CPU, NVIDIA CUDA, or Apple MPS; the dependency is optional and the device is explicit.

```bash
pip install -r requirements-acceleration.txt
# In .env, choose these together for a NEW database:
# EMBEDDING_BACKEND=local
# LOCAL_EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
# LOCAL_EMBEDDING_DEVICE=cpu
# LOCAL_EMBEDDING_CPU_THREADS=0
# LOCAL_EMBEDDING_REVISION=<tested-immutable-model-commit>
# EMBEDDING_DIM=384
```

Changing vector dimensions requires a reviewed migration and re-embedding. Changing models changes the embedding namespace; old vectors are not silently mixed. GPU inference is useful only when workload and batch size justify its overhead. The gateway still calls the LLM provider; local embeddings do not make generation offline. See [performance measurement](docs/performance.md) for repeatable measurement and capacity limits.

## Development and deployment

```bash
ruff check .
pytest -q
python scripts/evaluate.py validate
pip-audit -r requirements.lock
```

CI checks Python 3.12 and 3.13. Pull requests and main-branch updates also run integration checks against PostgreSQL, Neo4j, and Redis without paid model calls. The image and development installs use `requirements.lock`, an exact runtime package snapshot from the tested Python 3.12 image. `requirements.txt` remains the human-maintained version-bound source. Audit lock updates and pin the base image digest in the deployment release process.

Read [operations](docs/operations.md) before deploying. It covers migration quarantine, backup/restore, outbox lag, retention cleanup, failure recovery, and SLO measurement. Streaming, end-user identity federation, per-record sharing policies, automated PII redaction, and a complete data-subject deletion workflow are outside the current implementation.

```text
app/          API, isolated retrieval, provider controls, batching, metrics, outbox
migrations/   Versioned PostgreSQL schema and upgrade notes
examples/     Synthetic use-case fixtures
scripts/      Validation, comparisons, evaluation, embedding benchmark
tests/        Offline contracts and optional backing-service integration
docs/        Architecture, security, evaluation, performance, operations
```

# Changelog

## 0.3.0 — Unreleased

Prepared on the `feat/open-source-memory-platform` branch. This entry does not
announce a published tag, package, image, or hosted service.

### Added

- Scoped memory inspection, explicit insertion, correction, and deletion with
  optimistic revision checks. Corrections supersede previous facts and invalidate
  dependent cached/contextual results; graph updates and erasure are queued.
- Native SSE chat streaming with provisional text deltas, explicit errors, and
  a final event that confirms durable accounting/memory completion.
- Memory Console with scoped request analytics, real graph exploration, library
  search and lifecycle controls, complete/streaming chat, retrieval comparisons
  and safe runtime/integration views. A supervised local launcher starts the
  application with synthetic demo or configured inference and real backing stores.
- Configurable OpenAI-compatible generation alongside Anthropic, plus an
  explicitly limited text Chat Completions adapter. Unsupported fields are
  rejected; native SDKs retain the full gateway metadata.
- Source-installable async Python and TypeScript clients for native chat,
  streaming, and memory operations. Transport, framing, cancellation, and error
  tests run without model-provider credentials. Clients do not retry writes.
- Synthetic server-side support walkthroughs and an integration guide explaining
  authenticated scope binding, memory revisions, and provisional streaming text.
- A repeated no-memory/vector/graph evaluator with separate blinded review output
  and model-matched comparison metrics, plus a bounded HTTP load tool that records
  successes, failures, latency distributions, and generation-cost coverage.
- Apache-2.0 licensing, contribution and security guidance, and issue/PR templates.
- An evaluation-only pinned local transformer server for real generation
  comparisons without paid API keys; nonstreaming and loopback-only.

### Upgrade and deployment notes

- Apply the versioned memory lifecycle migrations before serving traffic; back
  up and review rollback constraints first. Schema changes do not run themselves
  when an operator updates an SDK.
- Native clients target the 0.3 API. Keep server and client versions aligned.
  Existing `/v1/chat` clients remain distinct from the new streaming contract.
- Tenant credentials stay on trusted servers. Memory APIs do not introduce
  end-user identity federation, cross-user sharing, or automatic PII redaction.
- Live-memory removal is distinct from eventual graph cleanup, content-free
  accounting, provider retention, and backup expiry.

### Evidence limits

Offline checks establish code and protocol behavior. They do not prove answer
quality, production reliability, or competitor superiority. Existing short warm
CPU/MPS measurements cover local embedding batching. Any newer end-to-end or
answer-quality report must identify its own workload and evidence.

## 0.2.0 — Foundation

The preceding source version introduced scoped graph/vector memory, exact cache
lookup, opt-in semantic cache reuse, durable outbox projection, bounded inference,
privacy-aware storage controls, operating metrics, evaluation fixtures, and local
embedding benchmarks. No historical package-publication date is asserted here.

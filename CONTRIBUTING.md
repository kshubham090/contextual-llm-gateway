# Contributing

Start with a small issue describing a reproducible problem or one concrete
workflow. The project focuses on scoped, inspectable memory for trusted
applications. New routing, storage, or provider features should justify their
operational cost and fit that boundary.

## Local checks

Use Python 3.12 or 3.13 for the gateway and Node.js 20.19 or newer for the
TypeScript client. From the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m pip install -e './sdk/python[test]'
ruff check .
pytest -q -m 'not integration'
python -m pytest sdk/python/tests -q
python scripts/evaluate.py validate
npm --prefix sdk/typescript ci
npm --prefix sdk/typescript test
node --test tests/browser/*.test.mjs
pip-audit -r requirements.lock --strict
```

These checks do not need paid model credentials. Tests marked `integration` need
PostgreSQL, Neo4j, and Redis; follow the service-integration workflow and
[operations guide](docs/operations.md). Keep real prompts, customer records,
credentials, and local environment files out of fixtures and pull requests.

## Reviewing changes

Keep changes focused and explain the user-visible result, scope boundaries,
failure behavior, and relevant verification. Add behavioral regression tests for
changes to authentication, retrieval isolation, memory lifecycle, persistence,
streaming, and concurrency. Test failures and cancellation as well as success.
Do not replace provider calls with fabricated timing/quality results.

Changes to the native API must update its schema, both SDKs, examples, and
documentation together. Preserve error information and do not add implicit
retries to non-idempotent writes. Partial streaming output must never be reported
as durable completion. The browser is outside the tenant credential trust boundary.

Before a storage change, provide versioned migrations, upgrade behavior, rollback
constraints, and tests against the affected stores. Correction/deletion must not
leave stale cache entries or permit a queued graph write to resurrect content.
The current pre-1.0 API may change in a minor release; describe breaking changes
and migration steps explicitly in the changelog. Do not assume a version bump
alone makes a deployment upgrade safe.

## Evidence and releases

Label synthetic mechanisms, real embedding inference, end-to-end gateway tests,
and answer-quality studies separately. Record model/provider versions, hardware,
workload, repetitions, cache state, concurrency, and relevant costs. Report
uncertainty and failures. Embedding throughput alone is not gateway throughput;
context provenance alone is not proof of correctness. See
[evaluation](docs/evaluation.md) and [performance](docs/performance.md).

Do not commit credentials or run paid/provider tests from untrusted pull-request
code. Release preparation should verify runtime dependency locks, migrations,
container configuration, SDK package contents, and security notes. Package
metadata and changelog entries do not mean a tag, container, PyPI, or npm release
has been published. Publishing is a separate maintainer action.

This project uses the [Apache License 2.0](LICENSE). Contributions are provided
under that license. Report vulnerabilities using [SECURITY.md](SECURITY.md),
not public issues containing exploit details or private data. Review ideas and
code respectfully, keep discussions relevant, and avoid personal attacks.

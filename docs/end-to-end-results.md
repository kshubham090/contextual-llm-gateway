# End-to-end evidence and its limits

On 2026-10-04 the complete HTTP gateway path processed **41,830 measured requests with no observed errors** across eight local concurrency profiles. These runs used real MiniLM embeddings, PostgreSQL/pgvector, Neo4j and Redis, with a **synthetic generation function that waits 40 ms and echoes context**. This measures application behavior around inference. It is not a hosted-LLM capacity result or an answer-quality claim.

## HTTP concurrency profiles

| Embedding device | Concurrent requests | Completed requests | Successful requests/s | HTTP p95 |
|---|---:|---:|---:|---:|
| CPU, four native threads | 1 | 441 | 14.70 | 82.30 ms |
| CPU, four native threads | 8 | 3,819 | 127.01 | 70.08 ms |
| CPU, four native threads | 32 | 10,967 | 364.13 | 108.33 ms |
| CPU, four native threads | 64 | 9,965 | 330.61 | 286.73 ms |
| Apple MPS | 1 | 407 | 13.54 | 80.84 ms |
| Apple MPS | 8 | 3,460 | 115.05 | 90.06 ms |
| Apple MPS | 32 | 5,719 | 189.95 | 283.92 ms |
| Apple MPS | 64 | 7,052 | 233.65 | 505.49 ms |

All profiles had zero HTTP/transport/protocol failures, no completion-cache hits, and four retrieved records per answer. Each profile scheduled for 30 seconds; reported throughput includes final response drain. Five warmup requests preceded each device's set and are excluded from this table. Complete per-attempt latency records are retained in the compressed raw reports.

The CPU run peaked at 32 concurrent requests among the tested settings. Increasing to 64 reduced successful throughput and raised p95. MPS was slower for this workload. These observations justify measuring whole-application behavior before selecting a device or increasing concurrency. They do not isolate the cause or establish optimal settings for other workloads.

## Configuration and reproducibility

- Apple M5, 10 physical CPU cores, 24 GiB memory; macOS 26.6.2, Python 3.14.7. Server and load generator ran on the same host.
- `sentence-transformers/all-MiniLM-L6-v2`, revision `1110a243fdf4706b3f48f1d95db1a4f5529b4d41`, 384 dimensions. Optional runtime versions match the [local embedding report](performance-results.md).
- One embedding worker, batch size 32, batching wait 5 ms. Explicit CPU/MPS selection. CPU uses four native threads; that setting does not configure MPS kernels.
- One gateway process; default admission limit 64, provider concurrency 32 and PostgreSQL pool maximum 10.
- Compose backing services: PostgreSQL 16/pgvector (1 CPU/1 GiB limit), Neo4j 5.26 (2 CPUs/2 GiB), Redis 7.4 (0.5 CPU/256 MiB). A dedicated 384-dimensional evaluation database was used.
- Four curated fictional support records in one scope, with graph similarity threshold 0.25. The threshold is a development setting for this local embedding model, not a tuned held-out claim. Other evaluation scopes are isolated.
- Repeated identical short question, graph retrieval, `store:false`, `use_cache:false`, and max output budget 128. Every request embeds and commits content-free accounting; neither completion reuse nor private embedding memoization removes this work.
- Ascending concurrency order, CPU first and MPS second; one completed trial per setting. The initial CPU profile overlapped the final local regression checks for part of its duration. Other desktop activity, temperature and power were not controlled. No device-to-device speedup or confidence interval is claimed.

Install optional acceleration requirements, select a fresh 384-dimensional database and the pinned model in `.env`, and launch the evaluation harness:

```bash
python scripts/demo_server.py --local-embeddings --port 8002
python scripts/compare_memory.py --url http://127.0.0.1:8002 \
  --scenario cedar-support --trials 3 --label 'real MiniLM; synthetic generation' \
  --output artifacts/fixture-check.json
```

The comparison report records its fresh scopes. Choose one of those scopes for the load run, configure rate limits sufficient for the intended experiment, and use its gateway token in `GATEWAY_API_KEY`:

```bash
python scripts/load_gateway.py --url http://127.0.0.1:8002 \
  --user USER_FROM_REPORT --feature FEATURE_FROM_REPORT --mode graph \
  --concurrency 1 8 32 64 --duration 30 --label 'hardware / device / synthetic generation' \
  --output artifacts/gateway-load.json
```

Stop the server before changing the embedding device. Keep the model/revision/dimension fixed to reuse this evaluation database. The script uses closed-loop workers, so it does not model independent arrival rates or eliminate coordinated omission. Larger histories, many active tenants, longer prompts, streaming time-to-first-token, failures, memory mutations and long soak tests require separate profiles. No NVIDIA CUDA hardware was measured.

## Raw reports

- [Machine-readable summary](../evals/gateway-local-summary.json), including uncompressed SHA-256 hashes.
- [CPU raw JSON, gzip](../evals/gateway-local-cpu.json.gz).
- [MPS raw JSON, gzip](../evals/gateway-local-mps.json.gz).

The synthetic comparison is a harness smoke check, not model evidence: echoing supplied facts trivially changes lexical coverage. Actual-generation evaluation must be reported separately with the model, immutable revision, prompt configuration, costs/usage, returned text and unfilled or independently completed human judgments.

## Actual local-model support comparison

A separate run used **real Qwen2.5-0.5B-Instruct generation**, with no synthetic answer function. The normal gateway called the loopback OpenAI-compatible evaluation server, using immutable model revision `7ae557604adf67be50417f59c2c2f167def9a775`, MPS float16, greedy decoding and a 256-output-token limit. MiniLM embeddings ran on CPU. Three fresh-scope trials each used four curated Cedar support memories, two held-out questions in three modes, and two empty-scope controls: 18 scored generations plus six controls.

| Retrieval mode | Scored answers | Mean expected-phrase coverage | Mean retrieved records | Total input + output tokens |
|---|---:|---:|---:|---:|
| None | 6 | 0% | 0 | 1,842 |
| Vector | 6 | 44.44% | 4 | 4,764 |
| Graph | 6 | 44.44% | 4 | 4,764 |

All six isolation controls returned no retrieved records. All six mode triplets used the same actual model/provider, and no request failed. No forbidden-phrase matches were detected. These are lexical measurements on fictional facts, **not accuracy or safety scores**. Human-review fields remain blank; no expert assessment is implied. Model pricing is unknown to the gateway, so all answers are marked unpriced; the zero sum of known prices must not be read as a measured total cost.

For example, a held-out question about CDR-409 received generic troubleshooting without memory. With either retrieval method, the model named cursor refresh and dry-run reconciliation, but omitted another expected fact. The two retrieval modes supplied the same four memories and produced the same expected-phrase coverage. **This experiment demonstrates memory use, but establishes no graph advantage over vector retrieval.** Larger connected histories and independent human evaluation are required for that claim.

Raw reports include HTTP latency and stage timing, but the short experiment, token-length differences, model warmup and shared desktop do not support a causal latency claim. Greedy decoding across three repetitions also does not create three independent language-model samples. The requested immutable revision and exact cached snapshot are recorded; the Transformers wrapper did not expose an independently resolved revision (`loaded_revision: null`).

- [Actual answers, sources, scores and timings](../evals/qwen-support-comparison.json).
- [Blinded review worksheet with unfilled judgments](../evals/qwen-support-blind-review.json).
- [Model, runtime and hardware metadata](../evals/qwen-support-environment.json).
- [Reproduce with the local evaluation server](local-model-evaluation.md).

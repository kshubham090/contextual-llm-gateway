# Measured local embedding results

On 2026-10-04, three repeated trials on an Apple M5 measured higher **warm local embedding throughput** with batches of 32 than with one text per invocation. The median paired throughput ratio was **4.998× on CPU** and **8.360× on Apple MPS**. Each comparison uses the same device and the same MiniLM model; these are not GPU-versus-CPU speedup ratios.

The benchmark performs real model inference on fixed short synthetic texts. It does not call the LLM provider, send requests through the HTTP gateway, traverse the memory graph, or score answer quality. These short trials do not establish sustained production capacity or latency objectives.

## Summary

| Device | Batch 1, median texts/s | Batch 32, median texts/s | Median paired throughput ratio | Median trial p95, batch 1 → 32 |
|---|---:|---:|---:|---:|
| CPU, 4 native threads | 308.27 | 1,502.88 | 4.998× | 264.891 → 46.057 ms |
| Apple MPS | 264.00 | 2,207.08 | 8.360× | 288.376 → 34.644 ms |

Throughput ratios are calculated within each trial and then summarized by their median. The ratio of the two median throughput columns is not the same statistic. The p95 columns are medians of three trial-level p95 values, not percentiles pooled over all requests. Three trials are insufficient to establish a confidence interval or rule out environmental effects.

| Device | Trial 1 ratio | Trial 2 ratio | Trial 3 ratio |
|---|---:|---:|---:|
| CPU | 4.539× | 4.998× | 5.466× |
| Apple MPS | 8.360× | 7.528× | 9.387× |

Every per-item versus batched vector comparison passed the cosine-similarity threshold of 0.999. The minimum cosine rounds to 1.0 in the reports; this does not imply exact bitwise equality or validate retrieval quality. Maximum reported component difference was zero at report precision for CPU and `4.5e-8` for MPS. Comparisons check batching agreement within each device, not semantic accuracy or equivalence across devices.

## Workload and environment

| Parameter | Value |
|---|---|
| Hardware | Apple M5, 10 physical CPU cores, 24 GiB memory |
| OS and Python | macOS 26.6.2 arm64; Python 3.14.7 |
| Libraries | Torch 2.14.1; Transformers 5.18.0; Sentence Transformers 5.7.0 |
| Model | `sentence-transformers/all-MiniLM-L6-v2` |
| Requested revision | `1110a243fdf4706b3f48f1d95db1a4f5529b4d41` |
| Output dimension | 384 |
| Trials | 3 per device |
| Texts per trial and mode | 512 |
| Maximum concurrent submitted items | 64 |
| Baseline | Batch size 1; no batching wait |
| Batched configuration | Batch size 32; 5 ms batching wait |
| Gateway embedding workers | 1; one local inference executor |
| Native CPU threads | 4 |
| Embedding memoization | Disabled |
| Model loading and warmup | Timed separately and excluded from throughput/latency |

The benchmark generates one short support/deployment sentence per item with a distinct case number. Each mode receives the same sequence. Baseline model warmup uses one text; batched warmup uses a representative full batch. Baseline runs before the batched measurement in each trial, so order effects are not eliminated. The longest measured mode lasted approximately 2.14 seconds; longer soak tests and realistic prompt-length distributions are still needed.

Latency begins after the benchmark concurrency semaphore admits an item and includes the embedding client's queue. It excludes time spent waiting for that admission semaphore, HTTP transport, authentication, PostgreSQL, Neo4j, generation, and persistence. Device power, other host activity, and thermal state were not controlled as laboratory variables.

The requested immutable revision is recorded and passed to model loading. `resolved_model_revision` is null because the model wrapper did not expose an independently resolved revision. This benchmark environment is separate from the Linux Python 3.12 application runtime lock.

## Reproduce

Install the optional acceleration dependencies and match the library versions above when comparing results. On a compatible Apple host, run each command three times with distinct output filenames:

```bash
python scripts/benchmark_embeddings.py \
  --backend local --device cpu --cpu-threads 4 --workers 1 \
  --model sentence-transformers/all-MiniLM-L6-v2 \
  --revision 1110a243fdf4706b3f48f1d95db1a4f5529b4d41 --dimension 384 \
  --requests 512 --concurrency 64 --batch-size 32 --batch-wait-ms 5 \
  --output artifacts/local-cpu-trial-1.json

python scripts/benchmark_embeddings.py \
  --backend local --device mps --cpu-threads 4 --workers 1 \
  --model sentence-transformers/all-MiniLM-L6-v2 \
  --revision 1110a243fdf4706b3f48f1d95db1a4f5529b4d41 --dimension 384 \
  --requests 512 --concurrency 64 --batch-size 32 --batch-wait-ms 5 \
  --output artifacts/local-mps-trial-1.json
```

A CPU host can reproduce the mechanism with its own performance profile. MPS requires supported Apple hardware; no NVIDIA CUDA performance was measured. Do not use these results to predict hosted embedding latency, LLM generation speed, graph-answer quality, or overall service throughput.

## Raw evidence

- [Machine-readable summary](../evals/local-inference-summary.json)
- CPU: [trial 1](../evals/local-cpu-trial-1.json), [trial 2](../evals/local-cpu-trial-2.json), [trial 3](../evals/local-cpu-trial-3.json)
- MPS: [trial 1](../evals/local-mps-trial-1.json), [trial 2](../evals/local-mps-trial-2.json), [trial 3](../evals/local-mps-trial-3.json)

The [performance guide](performance.md) explains queue/worker/thread tuning. The separate [graph evaluation protocol](evaluation.md) is needed to measure whether memory improves answers on a chosen domain.

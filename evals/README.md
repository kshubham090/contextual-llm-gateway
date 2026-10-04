# Recorded local inference measurements

These reports were produced on 2026-10-04 using actual local MiniLM inference on an Apple M5. They measure warm embeddings for short synthetic texts, with three repeated CPU trials and three MPS trials. They do not measure the HTTP gateway, graph-answer quality, LLM generation, or sustained production capacity.

Read the [methodology and interpretation](../docs/performance-results.md) before using the values. `local-inference-summary.json` summarizes the six trial reports. Throughput ratios are medians of paired trial ratios; reported summary p95 values are medians of trial-level p95 measurements.

# Evaluation protocol

The evaluation answers one narrow question: **does retrieval recover supplied synthetic facts without crossing the configured memory scope?** It also records latency, token usage, estimated generation cost, and provenance so quality and operating cost can be considered together.

It does not establish expert correctness, safety, production reliability, statistical significance, or superiority to another model. Bundled [local embedding measurements](performance-results.md) cover a separate inference benchmark. Paid-provider answer quality has not been measured in the bundled reports.

## Offline checks

```bash
python scripts/evaluate.py validate
pytest tests/test_evaluation.py -q
```

The validator checks the fixture version, explicit synthetic designation, disjoint scopes, IDs, held-out prompt uniqueness, and expected fact support in seeds. Tests cover phrase boundaries, aliases, malformed fixture conditions, error reporting, comparison order, private evaluation flags, isolation controls, and JSON serialization. They use a deterministic HTTP transport; this is test evidence about the harness, not model quality.

## Live experiment

Start the gateway and export `GATEWAY_API_KEY` as a valid tenant bearer token. This is a gateway credential; it is not the Anthropic or Voyage provider key.

```bash
python scripts/evaluate.py run \
  --scenario orbit-incident \
  --scenario quartz-data \
  --output artifacts/evaluation.json
```

For each selected scenario the runner:

1. Creates a unique user scope from the pack and run ID.
2. Writes four fact-bearing seed interactions with graph context injection and completion caching disabled. `store: true` still embeds, finds neighbors, and builds graph links, so seed generation stays independent while the memory graph accumulates.
3. Waits for the scoped graph count to include the committed seeds, with a bounded timeout.
4. Asks each held-out question twice. Completion caching is disabled and both answers use `store: false`. Graph order alternates by question to reduce a fixed order effect.
5. Asks in an unseeded user scope and an unseeded feature scope; each should return an empty `context_used` list.
6. Writes a JSON report, including transport errors and failed controls. A failed request is not silently scored as a poor answer.

Each pack makes ten generation requests: four seeds, four comparisons, and two isolation controls. All six packs make sixty, before any provider retries. Seed calls and isolation controls also incur provider costs, which appear in their individual metadata. The summary's generation cost covers scored answers only. External embedding and infrastructure charges are not included.

Use a fresh run ID for independent trials. To reuse a previously seeded scope:

```bash
python scripts/evaluate.py run --scenario orbit-incident --run-id trial-01 \
  --output artifacts/trial-01.json
python scripts/evaluate.py run --scenario orbit-incident --run-id trial-01 --skip-seed \
  --output artifacts/trial-01-repeat.json
```

Re-seeding the same scope adds memories and changes the workload. Long-running trials can also be affected by memory TTL. Failed partial seed runs should use a fresh ID. A nonzero exit means a fixture/input failure, transport error, or failed isolation control; a successful exit is not a model-quality certification.

## Report schema

The JSON has `schema_version`, `run_id`, timestamps, `fixture_sha256`, parameters, seed metadata, scored `results`, `isolation_controls`, `errors`, and `summary`.

Each scored result records the prompt, response, scenario/question IDs, exact scope, mode, gateway metadata, and lexical score. Metadata retains the actual routed model and context IDs; compare them before attributing a difference to retrieval. Summaries show mean expected-fact recall, forbidden matches, answers that received context, mean reported latency, reported generation cost, and unpriced answers for each mode. `recall_delta` is the mean graph-on minus graph-off difference over fully paired questions only; `paired_question_count` gives its denominator. Unpaired answers remain visible in the per-mode summaries but cannot create an artificial comparison delta.

**Lexical expected-fact recall:** a fact is found if any accepted phrase occurs after Unicode normalization, case folding, and punctuation/whitespace normalization. Each fact counts once. Boundaries prevent “8 minutes” from matching “18 minutes.” Missing paraphrases can be false negatives. An answer can mention every expected phrase while still being wrong.

**Forbidden fact matches:** phrases supplied as negative probes are recorded. This scorer does not understand negation; “do not bypass guards” still contains “bypass guards.” These are review signals, not automatic safety findings. Do not combine them into a polished-looking overall safety score.

## Interpreting outcomes

An answer with empty `context_used` provides no evidence of graph-memory benefit, even if it happens to contain an expected fact. A forbidden cross-scope marker can come from generation rather than actual retrieval; inspect provenance to distinguish those possibilities. Empty context in a negative control is a useful invariant, but does not prove exhaustive isolation.

Thresholds are embedding-model dependent. A default tuned for one embedding model may retrieve too little with another. Compare recall/precision on a development split before freezing parameters for a held-out run. Do not change thresholds or aliases based on final test answers without reporting that adaptation.

For a stronger study, run multiple fresh-scope trials with fixed model versions, control generation settings, include a vector-only baseline, add realistic authorized documents, record human blinded judgments, and estimate uncertainty. The legacy `scripts/evaluate.py` harness does not offer a vector-only comparison or a model judge. The newer three-way harness below supplies the vector baseline. Add contradiction, stale policy, prompt-injection, and tenant-overlap cases before making deployment claims.


## Three-way comparison in 0.3.0

`python scripts/compare_memory.py --scenario cedar-support --trials 3 --label "your model and hardware"` runs no memory, vector-only retrieval, and graph retrieval on the same held-out questions. It creates curated seeds via the memory API instead of generating seed answers, waits for graph projection, and uses a fresh user scope per scenario and trial. Vector and graph modes use the same embedding, ranking, and context budgets; graph mode additionally expands neighbors. Model/routing settings should remain fixed throughout a run.

Mode positions rotate across three consecutive trials, with a seeded initial permutation per question. Each scored response has `store:false` and `use_cache:false`. User and feature negative controls are recorded separately. Three-way complete triplets and model-matched triplets are reported separately; mismatched or missing provider/model identities cannot contribute to the paired graph-minus-vector fact-coverage metric. Failed calls remain errors, never zero-quality answers.

The report records fixture hash, configuration label, seeds, scope IDs, HTTP latency, actual models, costs, provenance, errors and scores. A separate `*-blind-review.json` omits mode labels and includes blank fields for human factual correctness and irrelevant/stale facts. Keep the report's review key away from blinded reviewers. It does not pre-populate human judgments. Raw answer text can still reveal the method, so blinding is imperfect.

For six packs, one trial performs 24 curated memory writes and 48 generation calls (36 scored answers and 12 negative controls), excluding retries internal to the gateway. Three trials multiply these counts by three. Curated writes incur embedding work; costs shown in the summary cover scored-answer generation only, with control costs preserved separately. Embedding, infrastructure and failed-call charges are not included. A fake or synthetic backend is valid for exercising the harness, never evidence of answer quality.

Lexical expected-fact coverage does not establish factual correctness, relevance, statistical significance or that a graph outperforms vector retrieval. The small bundled fixtures may give graph and vector modes the same context. Add larger connected histories, contradictory and superseded rules, overlap across tenants, and human adjudication before claiming an advantage. Do not tune thresholds on the held-out results. The lifecycle suite separately verifies correction, forgetting, stale graph replay and cross-scope exclusion with real backing services.

For paid-key-free **actual generation**, use the pinned, loopback-only [local model evaluation server](local-model-evaluation.md). It supports nonstreaming Chat Completions for controlled experiments; it is not a production serving engine. Its results remain specific to the model and fixtures.

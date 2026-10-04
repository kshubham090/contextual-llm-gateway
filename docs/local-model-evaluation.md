# Real local-model evaluation without paid API calls

`scripts/local_eval_provider.py` is an **evaluation-only**, loopback Chat Completions server. It runs actual transformer generation with the pinned model's chat template; it does not echo memory or manufacture token usage. It is separate from the production gateway and is not included in the production image. The small model is an inexpensive way to test answer generation through the full memory path, not a claim of frontier-model quality.

Install the existing optional inference requirements, then start the local model:

```bash
pip install -r requirements-acceleration.txt
HF_HOME=artifacts/model-cache python scripts/local_eval_provider.py \
  --model Qwen/Qwen2.5-0.5B-Instruct \
  --revision 7ae557604adf67be50417f59c2c2f167def9a775 \
  --device mps --port 8003 \
  --max-input-tokens 4096 --max-output-tokens 256 --timeout-seconds 120
```

The revision is mandatory and must be an immutable 40-character commit, not `main`. This public model is loaded with `trust_remote_code=False`, safetensors, and no Hugging Face authentication token. Initial startup downloads the pinned weights into the selected cache. Add `--local-files-only` for later runs that must use an already populated cache. `--model` is a trusted operator CLI setting; HTTP callers cannot select another repository.

Choose a device actually available on the machine. For CPU, use `--device cpu --cpu-threads 4`; CUDA requires a matching PyTorch installation. CPU inference uses float32 and MPS/CUDA use float16. `GET http://127.0.0.1:8003/health` records the model, requested and loaded revision, device, dtype, PyTorch/Transformers versions, thread count, token limits and active slot. Save that metadata beside the evaluation report. Generation is greedy (`do_sample=False`, one beam); this removes sampling randomness but does not promise identical floating-point results across hardware or runtime versions.

Run the **normal production gateway** in a separate terminal, configured with test-only backing stores and a fresh evaluation tenant. Its `.env` should already contain valid gateway authentication and database/Neo4j/Redis credentials. For a fresh database configured for 384-dimensional MiniLM embeddings:

```bash
GENERATION_BACKEND=openai_compatible \
OPENAI_BASE_URL=http://127.0.0.1:8003/v1 \
OPENAI_API_KEY= \
SIMPLE_MODEL=Qwen/Qwen2.5-0.5B-Instruct \
COMPLEX_MODEL=Qwen/Qwen2.5-0.5B-Instruct \
OPENAI_MAX_TOKENS_FIELD=max_tokens \
EMBEDDING_BACKEND=local \
LOCAL_EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2 \
LOCAL_EMBEDDING_REVISION=1110a243fdf4706b3f48f1d95db1a4f5529b4d41 \
LOCAL_EMBEDDING_DEVICE=cpu LOCAL_EMBEDDING_CPU_THREADS=4 EMBEDDING_DIM=384 \
GRAPH_SIMILARITY_THRESHOLD=0.25 \
PROVIDER_TIMEOUT_SECONDS=120 REQUEST_TIMEOUT_SECONDS=180 \
uvicorn app.main:app --host 127.0.0.1 --port 8002
```

Do not change the embedding dimension of an existing collection for this experiment. Use a dedicated database/schema and fresh scopes. Both model tiers intentionally name the same actual model, so routing cannot confound comparisons and there is no fallback to another model. The 0.25 graph similarity threshold reproduces the published run's frozen development setting for MiniLM; it is not a tuned held-out result. For other experiments, review thresholds on development examples and freeze them before the scored run; record any nondefault thresholds in the report's label/configuration manifest.

Set `GATEWAY_API_KEY` in the evaluation process environment to a valid token for the evaluation tenant; keep it out of command-line arguments. Then run the existing counterbalanced comparison:

```bash
python scripts/compare_memory.py \
  --url http://127.0.0.1:8002 --scenario cedar-support \
  --trials 3 --max-tokens 256 --timeout 180 \
  --label 'Actual Qwen2.5-0.5B-Instruct@7ae5576; MPS float16 greedy; MiniLM CPU pinned; graph threshold 0.25' \
  --output artifacts/qwen-support-comparison.json
```

This makes real generated answers for no memory, vector-only memory, and graph memory through the same normal gateway API. Curated fixture ingestion and isolation controls remain unchanged. The report contains raw answers, actual token usage, returned context IDs, errors and lexical scores; a separate file supplies blank blinded-review fields. Read the [evaluation protocol](evaluation.md) before interpreting scores. A result from one small model and one synthetic support fixture cannot establish broad answer quality or a graph advantage. Unknown gateway model pricing remains `null`; local compute, electricity, hardware, and embedding work are not priced by this harness.

## Deliberate operating limits

The server binds only `127.0.0.1`, has no `--host` option, and runs one process with one active native inference job and no waiting inference queue. The server returns 503 while that slot is occupied. It accepts only text conversations and a configured token budget. Input is bounded in bytes, characters and actual tokenizer tokens; output is bounded in generated tokens. Unknown parameters and `stream:true` return 422. The **production gateway** implements real streaming; this evaluation fixture deliberately supports only nonstream generation required by `compare_memory.py`.

An HTTP disconnect or deadline signals a cooperative stopping criterion. It is checked at generated-token boundaries and cannot interrupt a running CPU/GPU kernel or a long prefill immediately. Admission stays occupied until the native future actually finishes, preventing hidden executor backlogs. Shutdown joins native work; a hung native kernel may require terminating this disposable evaluation process. These constraints are why this script is an evaluation fixture rather than a general inference server.

The lightweight `tests/test_local_eval_provider.py` suite validates admission under cancellation/timeouts, native cleanup, disconnects, real-worker usage propagation, input limits, unsupported requests, revision pinning and loopback CLI restrictions without downloading a model. Those tests validate the wrapper, not language-model quality.

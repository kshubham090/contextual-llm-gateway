# Generation providers and streaming API

The gateway adds scoped memory, retrieval controls, quotas, and durable call accounting around text generation. It supports Anthropic and a configurable Chat Completions HTTP backend. The `/v1/chat/completions` adapter implements the text subset below; it is not a complete replacement for the OpenAI API.

The wire format follows the official [Chat Completions create reference](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create) and [streaming events reference](https://developers.openai.com/api/reference/resources/chat/subresources/completions/streaming-events). Those references describe the upstream API; the explicit restrictions here describe this gateway.

## Provider configuration

Anthropic remains the default:

```dotenv
GENERATION_BACKEND=anthropic
ANTHROPIC_BASE_URL=https://api.anthropic.com
ANTHROPIC_API_KEY=<server-side-secret>
SIMPLE_MODEL=claude-haiku-4-5
COMPLEX_MODEL=claude-sonnet-4-5
```

A hosted Chat Completions endpoint:

```dotenv
GENERATION_BACKEND=openai_compatible
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_API_KEY=<server-side-secret>
OPENAI_MAX_TOKENS_FIELD=max_completion_tokens
OPENAI_STREAM_USAGE=true
SIMPLE_MODEL=<available-model-id>
COMPLEX_MODEL=<available-model-id>
```

For a host-run vLLM server, use `OPENAI_BASE_URL=http://127.0.0.1:8001/v1`; for a host-run Ollama server, use `http://127.0.0.1:11434/v1`. Choose the actual served model IDs and set `OPENAI_MAX_TOKENS_FIELD=max_tokens` when that is the token-budget field the server supports. A local server without authentication may leave `OPENAI_API_KEY` empty. The same model may fill both routing tiers; that disables model fallback. The gateway does not download generation models or start an inference server. Inside Docker, loopback points to the gateway container: use a reachable service name or host address.

The OpenAI-compatible backend sends text `messages`, `model`, `stream`, the configured token-budget field, and, for streaming, `stream_options.include_usage=true`. If a local server rejects the usage option, set `OPENAI_STREAM_USAGE=false`. Unknown usage yields `usage_available=false`, token counters of zero as placeholders, and `cost=null`; the gateway does not infer counts from text or claim that generation was free. Configure `MODEL_PRICING` for model IDs whose actual rates you know. Cache hits perform no provider generation and report zero provider usage.

Provider URLs must be HTTP(S) with no embedded credentials, query, or fragment. Secrets belong in their separate environment variables. Provider identity, base URL, routing model, conversation, retrieval policy, token budget, embedding space, and memory lifecycle epoch separate completion cache eligibility.

## Native requests

`POST /v1/chat` returns the complete native `ChatResponse`. `POST /v1/chat/stream` accepts the same body and returns `text/event-stream`.

```json
{
  "prompt": "What changed in the customer's deployment policy?",
  "user_id": "customer-17",
  "feature_tag": "support",
  "retrieval_mode": "graph",
  "use_cache": true,
  "cache_mode": "exact",
  "store": true,
  "max_tokens": 512
}
```

Authenticate with `Authorization: Bearer <gateway-token>`. The token supplies the tenant. A trusted application supplies `user_id` and `feature_tag`; do not expose a shared tenant token to mutually untrusted browser users.

Optional `system_prompt` is text up to 16,000 characters. Optional `history` contains at most 32 alternating `user`/`assistant` text messages, in complete pairs before the current `prompt`. The total conversation is limited to 64,000 characters. Optional `model` selects one of the two configured routing models; omission uses the existing gateway routing policy. Fallback may use the other configured model.

Retrieval controls:

| `retrieval_mode` | Context used during generation |
| --- | --- |
| `none` | No historical context |
| `vector` | Ranked active PostgreSQL vector candidates, without graph expansion |
| `graph` | Graph-expanded candidates plus vector seeds, revalidated and hydrated from PostgreSQL |

Legacy `use_graph=true` selects graph retrieval; `false` selects none when `retrieval_mode` is omitted. If both are supplied, `use_graph` must equal whether `retrieval_mode` is `graph`; contradictions return 422. To benchmark generation without memory, use `retrieval_mode=none` and `use_cache=false`. `store=true` still embeds and finds neighbors for future memory links. Also set `store=false` to skip those writes and their embedding work.

`use_cache` is independent of retrieval. Exact cache lookup uses the full scoped prompt, budget, configuration, and TTLs before requesting an embedding. Semantic reuse requires explicit `cache_mode=semantic`. Approximate retrieval remains subject to ANN recall limits.

## Native streaming contract

A successful generated stream contains genuine provider text fragments followed by a durable final response:

```text
event: delta
data: {"delta":"The customer ","model":"configured-model","provider":"openai_compatible"}

event: delta
data: {"delta":"changed the rollout window.","model":"configured-model","provider":"openai_compatible"}

event: final
data: {"response":"The customer changed the rollout window.","meta":{"durable":true,"call_id":"...","cache_hit":false,"memory_write":"queued"}}

```

The final example abbreviates metadata; the actual event contains the complete native `ChatResponse`, including tokens, estimated cost, timings, selected context IDs, retrieval mode, and memory epoch. `memory_write=queued` means the PostgreSQL call/outbox transaction committed; the graph projection may still be pending. With `store=false`, accounting still commits but prompt, answer, embedding, and graph event are not stored, and `memory_write=disabled`.

A cache hit emits one delta containing the whole cached answer and then a final event with `meta.cache_hit=true`. This is explicitly cached delivery, not simulated token generation.

After provisional text has begun, errors arrive in band:

```text
event: error
data: {"error":{"code":"inference_error","message":"Inference service unavailable"},"partial":true,"durable":false}

```

There is no final event after an error. The native endpoint has no `[DONE]` marker; a verified final event is its success signal. Errors before the first event use ordinary HTTP status responses: 409 for a memory revision conflict, 429 for quotas, 503 for capacity/storage, 504 for the request deadline, 502 for inference/protocol errors, and 422 for unsupported requests. Detailed upstream exception messages and credentials are not returned.

Deltas remain provisional until final. Corrections/deletions advance the scope epoch; a completion using an old epoch cannot become new active memory or return a successful final response. Already streamed text cannot be retracted. Source records are checked again at commit, and a generated answer or cache alias cannot outlive its earliest source expiry. Expired or removed sources cause a conflict instead of renewing old facts. Completed provider usage may still be recorded without content when memory changes during generation. Partial, interrupted provider streams do not get completed-call accounting; the upstream provider can still charge for work already performed.

## Text Chat Completions adapter

`POST /v1/chat/completions` supports:

- Required `model`: `gateway-auto` or either configured routing model ID.
- Required `messages`: one optional leading text `system` message, alternating text `user`/`assistant` history pairs, and a final `user` message.
- Required `gateway`: `user_id`, `feature_tag`, plus optional `retrieval_mode`, `use_cache`, `cache_mode`, and `store` controls.
- One optional token limit, `max_completion_tokens` or legacy `max_tokens`, in the range 1–8192. Both together are rejected.
- `stream` and, only with streaming, `stream_options.include_usage`.
- `n=1` only.

Images, audio, tools/function calls, tool messages, developer messages, structured outputs, sampling controls such as temperature, logprobs, multiple choices, and unknown fields are rejected with 422 rather than silently ignored. Authentication and validation errors use the gateway's FastAPI error format, not the full OpenAI error schema. This adapter does not implement `/v1/models`, Responses, or other OpenAI resources.

An OpenAI Python client can supply the required scope using `extra_body`:

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8000/v1", api_key="<gateway-token>")
response = client.chat.completions.create(
    model="gateway-auto",
    messages=[{"role": "user", "content": "Summarize our support history."}],
    max_completion_tokens=512,
    extra_body={"gateway": {"user_id": "customer-17", "feature_tag": "support"}},
)
print(response.choices[0].message.content)
```

The OpenAI client is optional and is not a gateway runtime dependency. Native Python and TypeScript clients live under [`sdk/`](../sdk/).

Non-stream responses contain standard `chat.completion` fields and additional `gateway` metadata. Streaming uses actual SSE `chat.completion.chunk` objects with stable `id` and `created`, an initial assistant role delta, subsequent real content deltas, and a finish-reason chunk. The finish chunk includes `gateway.durable=true` after the transaction commits. If requested, an additional chunk has `choices=[]` and aggregate `usage`; `[DONE]` follows. When upstream usage is missing, usage is `null` and `gateway.usage_available=false`.

An interrupted stream may have no usage or finish chunk. A gateway failure after text produces a safe `{error:..., gateway:{partial:true,durable:false}}` SSE object followed by `[DONE]`. Treat such errors as failure; `[DONE]` alone is not durable confirmation. Generic OpenAI clients may discard custom metadata, so applications needing memory-write confirmation should use the native SDK or retain the final chunk's `gateway` extension.

## Deadlines, backpressure, and delivery limits

`PROVIDER_MAX_CONCURRENCY` bounds provider work and the waiting queue. Both complete and streaming requests use the same per-model circuit breaker and bounded attempt deadlines. Only transient failures before any text can trigger the single configured fallback; once text is emitted, the gateway cannot switch models mid-answer. `PROVIDER_MAX_RESPONSE_BYTES` bounds parsed upstream responses and streams. A bounded eight-delta queue applies client backpressure without accumulating an unbounded answer backlog.

The overall request deadline includes retrieval, generation, backpressure, and persistence. A deadline releases upstream resources even if the client stops draining. Closing the connection cancels upstream work and releases admission. A disconnect racing a completed database commit may leave a durable record without a delivered final event; this API does not provide exactly-once delivery or a client idempotency key. Check the returned call ID when available, and treat missing final confirmation as an unknown delivery outcome.

Memory mutation makes earlier embedding-cache namespaces unreachable through the scope epoch. Old process-local vector entries expire within `EMBEDDING_CACHE_TTL_SECONDS`; this is separate from deletion of prompt/response data in PostgreSQL and eventual graph tombstone projection. Request content is not used as the memoization key and `store=false` disables embedding memoization and singleflight sharing.

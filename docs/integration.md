# Integrating a support assistant

Lowq is a gateway for trusted application servers. A tenant bearer key authorizes
the application to select any user and feature within that tenant. Your server
must authenticate the person, derive their `user_id`, and select an authorized
`feature_tag` before calling Lowq. A customer-supplied user ID is not authorization.
Keep keys out of browser/mobile bundles, logs, URLs, and source control.

The Python client requires Python 3.11 or newer; the TypeScript client requires
Node.js 20.19 or newer. The gateway itself requires Python 3.12 or newer.
Both clients use the native API below. Their package metadata
is versioned `0.3.0`; neither package has been published to a registry. Source
installation is available now:

```bash
python -m pip install ./sdk/python
npm --prefix sdk/typescript ci
npm --prefix sdk/typescript run build
```

The Python package imports as `lowq_client`. Install the built TypeScript package
into a server project with `npm install /absolute/path/to/sdk/typescript`, then
import from `@lowq/client`. SDK tests use fake transports and no model credentials:

```bash
python -m pip install -e './sdk/python[test]'
python -m pytest sdk/python/tests -q
npm --prefix sdk/typescript test
```

## Chat and streaming

`POST /v1/chat` and `POST /v1/chat/stream` accept the same body. Required fields
are `prompt`, `user_id`, and `feature_tag`; `store`, `use_graph`, `use_cache`,
`cache_mode`, and `max_tokens` are optional. For explicit retrieval selection, use
`retrieval_mode: "none" | "vector" | "graph"` and omit `use_graph`; contradictory
values are rejected. `system_prompt`, alternating user/assistant `history` pairs,
and a configured `model` selection are also supported. The ordinary endpoint returns
`{response, meta}`. `context_used` identifies supplied source interactions, not
proof that each statement in an answer is correct.

```python
import os
from lowq_client import AsyncGateway

# This runs on your server, after authenticating customer-42.
async def answer_support_question(prompt: str):
    async with AsyncGateway(api_key=os.environ["GATEWAY_API_KEY"]) as gateway:
        return await gateway.chat({
            "prompt": prompt,
            "user_id": "customer-42",
            "feature_tag": "support",
            "store": True,
        })
```

Reuse a client across requests in a real server. A fixed ID above is illustrative;
production code binds it to an authenticated account. Strict scopes do not provide
team-wide sharing. A `store: false` request may still read existing memory and
retains content-free accounting; provider processing and retention are separate.

Native streaming returns `Content-Type: text/event-stream`:

```text
event: delta
data: {"delta":"Refresh the cursor.","model":"configured-model","provider":"configured-provider"}

event: final
data: {"response":"Refresh the cursor.","meta":{"durable":true,"context_used":["source-call-id"]}}

```

The final example abbreviates metadata. Delta events contain actual provider text;
an exact cache hit may emit the whole cached answer in one delta. Only a `final`
event confirms that accounting and any memory write committed. Neo4j projection
may still be queued. Deltas can precede a server failure:

```text
event: error
data: {"error":{"code":"storage_unavailable","message":"Required storage unavailable"},"partial":true,"durable":false}

```

The SDK raises a `GatewayStreamError` for that event and a `GatewayProtocolError`
if the stream ends without final. Treat visible partial text as provisional. There
is no automatic fallback after the first text delta and no client auto-retry.
Disconnecting cancels upstream work when the server observes it; a client timeout
or disconnect can still race a completed server transaction.

```typescript
import { GatewayClient } from "@lowq/client";

const gateway = new GatewayClient({ apiKey: process.env.GATEWAY_API_KEY! });
const controller = new AbortController();
for await (const event of gateway.streamChat({
  prompt: "What should I do next for CDR-409?",
  user_id: "customer-42", feature_tag: "support",
}, { signal: controller.signal })) {
  if (event.type === "delta") process.stdout.write(event.delta);
  else console.log("\nCompleted:", event.response.meta.call_id);
}
```

TypeScript aborts a pending read with `controller.abort()` and preserves native
`AbortError`/`TimeoutError`. Breaking its loop closes the reader. Python uses
`async with gateway.stream_chat(request) as events`; exiting this context closes
the stream even after a break. Cancel the consuming asyncio task to interrupt a
pending read. A Python `timeout` sets HTTPX's operation timeouts; TypeScript
`timeoutMs` covers the entire request, including stream consumption.

## Inspecting, correcting, and deleting memory

All memory requests require an exact `user_id` and `feature_tag`. The tenant is
derived from the bearer token. The clients never supply a tenant ID in JSON.

| Operation | Native endpoint | Revision required |
|---|---|---|
| List a page | `GET /v1/memories` | None; returns `scope_revision` and `next_cursor` |
| Inspect one | `GET /v1/memories/{id}` | None; returns record `revision` |
| Add an explicit memory | `POST /v1/memories` | `expected_scope_revision` in body |
| Correct a memory | `PATCH /v1/memories/{id}` | `expected_revision` in body |
| Delete one | `DELETE /v1/memories/{id}` | `expected_revision` in query |
| Delete this scope | `DELETE /v1/memories` | `expected_scope_revision` in query |

Lists accept `limit` and `cursor`. Pass `next_cursor` unchanged for the next page.
Mutations carry scope in JSON for POST/PATCH and query parameters for DELETE.
Corrections accept the replacement `prompt` and `response`; they supersede the
old record and return `{memory, scope_revision, invalidated_count, graph_write}`.
Deletion returns `{deleted_ids, deleted_count, deleted_ids_truncated,
invalidated_count, scope_revision, graph_write}`. The list of affected IDs is
capped at 1,000; use `deleted_count` for the total and inspect
`deleted_ids_truncated` before treating that list as complete.
An HTTP 409 signals that the revision changed: read again and let your application
resolve the conflict. Never blindly retry a correction against a new revision.

```python
async def correct_support_memory(gateway, memory_id: str, prompt: str, answer: str):
    scope = {"user_id": "customer-42", "feature_tag": "support"}
    current = await gateway.get_memory(memory_id, **scope)
    return await gateway.correct_memory(
        memory_id, **scope, prompt=prompt, response=answer,
        expected_revision=current["revision"],
    )

async def delete_reviewed_memory(gateway, memory_id: str, revision: int):
    return await gateway.delete_memory(
        memory_id, user_id="customer-42", feature_tag="support",
        expected_revision=revision,
    )
```

Provide these operations only through an authorized application workflow.
Corrections and deletions remove affected content from live PostgreSQL memory and
invalidate dependent context/cache entries. Graph erasure is asynchronous;
`graph_write: "queued"` does not confirm it finished. Content-free accounting,
provider logs, and backup expiry follow separate policies. These APIs alone are
not a complete data-subject deletion or compliance workflow.

## Running the synthetic support examples

With a configured gateway running, export `GATEWAY_API_KEY` in the server/terminal
environment and optionally `GATEWAY_BASE_URL`. Then run either example:

```bash
python examples/support-python.py
node examples/support-typescript.mjs
```

Each example creates a fresh synthetic user scope, seeds and corrects a fictional
connector fact, streams one follow-up answer, and prints provenance plus memory
status. The follow-up calls the configured generation provider and may cost money.
No real customer data is used. Append `--cleanup` to delete that example's fresh
scope after inspecting its output; accounting and backup policies still apply.

The examples demonstrate integration and lifecycle semantics. They are not an
answer-quality benchmark or production support system. Use the
[evaluation protocol](evaluation.md) to measure quality and operating cost for
your workload; small local embedding benchmarks do not establish gateway speed.

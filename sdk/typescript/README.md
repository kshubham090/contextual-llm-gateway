# Lowq TypeScript client

A typed, server-side client for Lowq's native chat, streaming, and scoped memory
APIs. Requires Node.js 20.19 or newer, uses native Fetch, and has no runtime
dependencies. This package has not been published to npm.

From the repository root:

```bash
npm --prefix sdk/typescript ci
npm --prefix sdk/typescript run build
```

To install in a separate server application, use
`npm install /absolute/path/to/contextual-llm-gateway/sdk/typescript` after building.

```typescript
import { GatewayClient } from "@lowq/client";

const gateway = new GatewayClient({ apiKey: process.env.GATEWAY_API_KEY! });
// Derive the scope from your server's authenticated identity and authorization.
const request = {
  prompt: "What did we try for my connector issue?",
  user_id: "customer-42",
  feature_tag: "support",
};
const abort = new AbortController();
for await (const event of gateway.streamChat(request, { signal: abort.signal })) {
  if (event.type === "delta") process.stdout.write(event.delta);
  else console.log("\nSources:", event.response.meta.context_used);
}
```

`await gateway.chat(request)` returns `{response, meta}`. Streaming yields `delta`
events and one `final` event containing the complete response. Partial text is
provisional; only final confirms durable completion. Breaking the loop cancels the
reader. An `AbortSignal` cancels a pending request/read; native `AbortError` or
`TimeoutError` is preserved. `timeoutMs` controls the total request and streaming
timeout (default 120,000 ms).

Memory methods are `listMemories`, `getMemory`, `createMemory`, `correctMemory`,
`deleteMemory`, and `deleteScope`. Scope and optimistic revision guards are
explicit. See [the integration guide](../../docs/integration.md).

`GatewayError` exposes `statusCode`, `requestId`, `retryAfter`, and `details`.
`GatewayStreamError` exposes `code`, `partial`, and `requestId`.
`GatewayTransportError` means delivery may be uncertain; `GatewayProtocolError`
means malformed responses or a stream without final confirmation. No operation
is retried automatically. HTTP redirects are rejected.

Keep tenant keys on the server. Do not import this package into a browser bundle
or pass a gateway key to an end user. Browser construction is rejected as a guard;
that check cannot protect a secret already bundled into client code.

```bash
npm --prefix sdk/typescript test
```

Apache-2.0 licensed. This client targets the 0.3 native API; it is not an OpenAI SDK adapter.

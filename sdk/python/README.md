# Lowq Python client

An async, typed client for Lowq's native chat, streaming, and scoped memory APIs.
Requires Python 3.11 or newer. The gateway server requires Python 3.12 or newer.
The only runtime dependency is HTTPX. This package has not been published to PyPI.

From the repository root:

```bash
python -m pip install ./sdk/python
```

Keep the gateway key in your application's server environment. The application
must derive `user_id` from the authenticated user and choose the authorized
`feature_tag`; do not copy these from an untrusted request body.

```python
import asyncio
import os
from lowq_client import AsyncGateway

async def main():
    async with AsyncGateway(api_key=os.environ["GATEWAY_API_KEY"]) as gateway:
        request = {
            "prompt": "What did we try for my connector issue?",
            "user_id": "customer-42",
            "feature_tag": "support",
        }
        async with gateway.stream_chat(request) as events:
            async for event in events:
                if event["type"] == "delta":
                    print(event["delta"], end="", flush=True)
                else:
                    print("\nSources:", event["response"]["meta"]["context_used"])

asyncio.run(main())
```

`await gateway.chat(request)` returns `{response, meta}`. Streaming yields `delta`
events and one `final` event whose `response` contains the complete chat response.
Only final confirms durable completion. Use the streaming context manager even
when stopping early; it releases the connection. Cancel the consuming asyncio task
to abort a pending read. The default HTTPX operation timeout is 120 seconds and
can be changed with `timeout=`.

Memory methods are `list_memories`, `get_memory`, `create_memory`,
`correct_memory`, `delete_memory`, and `delete_scope`. Every method requires
`user_id` and `feature_tag`. Corrections/deletions require the revision returned
by a preceding read; see [the integration guide](../../docs/integration.md).

`GatewayError` preserves `status_code`, `request_id`, `retry_after`, and `details`.
`GatewayStreamError` preserves the error `code` and `partial` flag.
`GatewayTransportError` indicates uncertain delivery; `GatewayProtocolError`
indicates an invalid response or missing final event. No operation is retried
automatically. A timeout or disconnect does not prove the server did no work.

```bash
python -m pip install -e './sdk/python[test]'
python -m pytest sdk/python/tests -q
```

Apache-2.0 licensed. This client targets the 0.3 native API; it is not an OpenAI SDK adapter.

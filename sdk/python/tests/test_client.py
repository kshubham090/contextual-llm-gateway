import asyncio
import json

import httpx
import pytest
from lowq_client import (
    AsyncGateway,
    GatewayError,
    GatewayProtocolError,
    GatewayStreamError,
    GatewayTransportError,
)

REQUEST = {"prompt": "Previous connector issue?", "user_id": "user-7", "feature_tag": "support"}
FINAL = {"response": "Refresh the cursor. ☀", "meta": {"call_id": "call-1", "durable": True}}


class Chunks(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks = chunks
        self.closed = False

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk

    async def aclose(self):
        self.closed = True


def stream_response(payload, *, chunk_size=1, headers=None):
    payload = payload.encode("utf-8")
    stream = Chunks([payload[i:i + chunk_size] for i in range(0, len(payload), chunk_size)])
    return httpx.Response(200, stream=stream, headers={
        "content-type": "text/event-stream; charset=utf-8", "x-request-id": "req-1", **(headers or {}),
    }), stream


def client(handler):
    return AsyncGateway(api_key="server-test-key", base_url="https://gateway.test/prefix/",
                        transport=httpx.MockTransport(handler))


async def test_chat_scopes_auth_and_path():
    def handle(req):
        assert req.method == "POST"
        assert req.url.path == "/prefix/v1/chat"
        assert req.headers["authorization"] == "Bearer server-test-key"
        assert json.loads(req.content) == REQUEST
        return httpx.Response(200, json=FINAL)

    async with client(handle) as gateway:
        assert await gateway.chat(REQUEST) == FINAL


@pytest.mark.parametrize("status,detail", [(409, "Memory changed"), (429, "Rate limit exceeded"),
                                          (422, [{"msg": "Required field"}]), (302, None)])
async def test_http_errors_preserve_details_without_retry(status, detail):
    calls = 0

    def handle(req):
        nonlocal calls
        calls += 1
        return httpx.Response(status, json={"detail": detail}, headers={
            "x-request-id": "req-error", "retry-after": "3", "location": "https://elsewhere.test",
        })

    async with client(handle) as gateway:
        with pytest.raises(GatewayError) as exc:
            await gateway.chat(REQUEST)
    assert calls == 1
    assert exc.value.status_code == status
    assert exc.value.request_id == "req-error"
    assert exc.value.retry_after == "3"
    assert exc.value.details == {"detail": detail}


async def test_transport_error_is_not_retried():
    calls = 0

    def handle(req):
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("test connection failure", request=req)

    async with client(handle) as gateway:
        with pytest.raises(GatewayTransportError):
            await gateway.chat(REQUEST)
    assert calls == 1


@pytest.mark.parametrize("body", ["not json", "[]", '{"response": 3, "meta": {}}'])
async def test_invalid_chat_response(body):
    async with client(lambda req: httpx.Response(200, content=body)) as gateway:
        with pytest.raises(GatewayProtocolError):
            await gateway.chat(REQUEST)


async def test_sse_byte_boundaries_crlf_unicode_comments_and_multiline_data():
    payload = '\ufeff: comment\r\nevent: delta\r\ndata: {"delta":\r\ndata: "☀"}\r\n\r\n'
    payload += 'event: final\r\ndata: ' + json.dumps(FINAL, ensure_ascii=False) + '\r\n\r\n'
    response, stream = stream_response(payload)

    def handle(req):
        assert req.url.path == "/prefix/v1/chat/stream"
        assert req.headers["accept"] == "text/event-stream"
        return response

    async with client(handle) as gateway:
        async with gateway.stream_chat(REQUEST) as events:
            received = [event async for event in events]
    assert received == [{"type": "delta", "delta": "☀"}, {"type": "final", "response": FINAL}]
    assert stream.closed


async def test_sse_error_preserves_partial_state():
    payload = 'event: delta\ndata: {"delta":"partial"}\n\n'
    payload += ('event: error\ndata: {"error":{"code":"storage_unavailable",'
                '"message":"Storage unavailable"},"partial":true,"durable":false}\n\n')
    response, stream = stream_response(payload)
    async with client(lambda req: response) as gateway:
        async with gateway.stream_chat(REQUEST) as events:
            assert (await anext(events))["delta"] == "partial"
            with pytest.raises(GatewayStreamError) as exc:
                await anext(events)
    assert exc.value.code == "storage_unavailable"
    assert exc.value.partial is True
    assert exc.value.durable is False
    assert exc.value.request_id == "req-1"
    assert stream.closed


@pytest.mark.parametrize("payload", [
    'event: delta\ndata: {"delta":"unfinished"}\n\n',
    'event: delta\ndata: not-json\n\n',
    'event: delta\ndata: {"delta":17}\n\n',
    'event: final\ndata: {"response":"x","meta":{}}\n\n',
    'event: final\ndata: {"response":"x","meta":{"durable":true}}\n',
])
async def test_malformed_or_incomplete_stream_is_not_success(payload):
    response, stream = stream_response(payload, chunk_size=7)
    async with client(lambda req: response) as gateway:
        with pytest.raises(GatewayProtocolError):
            async with gateway.stream_chat(REQUEST) as events:
                _ = [event async for event in events]
    assert stream.closed


async def test_early_exit_closes_stream():
    response, stream = stream_response('event: delta\ndata: {"delta":"start"}\n\n')
    async with client(lambda req: response) as gateway:
        async with gateway.stream_chat(REQUEST) as events:
            async for event in events:
                assert event["type"] == "delta"
                break
        assert stream.closed


async def test_cancellation_closes_pending_stream():
    entered = asyncio.Event()

    class Pending(httpx.AsyncByteStream):
        closed = False

        async def __aiter__(self):
            entered.set()
            await asyncio.Event().wait()
            yield b"unreachable"

        async def aclose(self):
            self.closed = True

    stream = Pending()
    response = httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream)
    async with client(lambda req: response) as gateway:
        async def consume():
            async with gateway.stream_chat(REQUEST) as events:
                return [event async for event in events]

        task = asyncio.create_task(consume())
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert stream.closed


async def test_stream_http_error_and_wrong_content_type():
    async with client(lambda req: httpx.Response(401, json={"detail": "Unauthorized"})) as gateway:
        with pytest.raises(GatewayError) as exc:
            async with gateway.stream_chat(REQUEST):
                pass
        assert exc.value.status_code == 401
    async with client(lambda req: httpx.Response(200, json=FINAL)) as gateway:
        with pytest.raises(GatewayProtocolError):
            async with gateway.stream_chat(REQUEST):
                pass


async def test_stream_read_failure_is_transport_error_and_closes():
    class Broken(Chunks):
        async def __aiter__(self):
            yield b'event: delta\ndata: {"delta":"partial"}\n\n'
            raise httpx.ReadError("stream disconnected")

    stream = Broken([])
    response = httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream)
    async with client(lambda req: response) as gateway:
        with pytest.raises(GatewayTransportError):
            async with gateway.stream_chat(REQUEST) as events:
                _ = [event async for event in events]
    assert stream.closed


async def test_extended_native_request_is_transmitted_without_changes():
    request = {
        **REQUEST, "retrieval_mode": "vector", "system_prompt": "Use supplied evidence.",
        "history": [{"role": "user", "content": "hi"}, {"role": "assistant", "content": "hello"}],
        "model": "configured-model",
    }

    def handle(req):
        assert json.loads(req.content) == request
        return httpx.Response(200, json=FINAL)

    async with client(handle) as gateway:
        await gateway.chat(request)


async def test_memory_paths_scopes_pagination_and_revision_guards():
    seen = []

    def handle(req):
        seen.append(req)
        return httpx.Response(200, json={"items": [], "next_cursor": None, "scope_revision": 4})

    scope = {"user_id": "user-7", "feature_tag": "support"}
    async with client(handle) as gateway:
        await gateway.list_memories(**scope, limit=7, cursor="cursor+with/slash=")
        await gateway.get_memory("memory-1", **scope)
        await gateway.create_memory(**scope, prompt="rule", response="fact", expected_scope_revision=4)
        await gateway.correct_memory("memory-1", **scope, prompt="rule", response="new", expected_revision=2)
        await gateway.delete_memory("memory-1", **scope, expected_revision=2)
        await gateway.delete_scope(**scope, expected_scope_revision=4)
    assert [(req.method, req.url.path) for req in seen] == [
        ("GET", "/prefix/v1/memories"), ("GET", "/prefix/v1/memories/memory-1"),
        ("POST", "/prefix/v1/memories"), ("PATCH", "/prefix/v1/memories/memory-1"),
        ("DELETE", "/prefix/v1/memories/memory-1"), ("DELETE", "/prefix/v1/memories"),
    ]
    assert seen[0].url.params["cursor"] == "cursor+with/slash="
    assert seen[0].url.params["limit"] == "7"
    assert json.loads(seen[3].content) == {
        **scope, "prompt": "rule", "response": "new", "expected_revision": 2,
    }
    assert seen[4].url.params["expected_revision"] == "2"
    assert seen[5].url.params["expected_scope_revision"] == "4"
    for req in seen:
        content = json.loads(req.content) if req.content else dict(req.url.params)
        assert content["user_id"] == scope["user_id"]
        assert content["feature_tag"] == scope["feature_tag"]


async def test_empty_scope_rejected_without_network():
    async with client(lambda req: pytest.fail("No request expected")) as gateway:
        with pytest.raises(ValueError):
            await gateway.list_memories(user_id="", feature_tag="support")


@pytest.mark.parametrize("url", ["file:///tmp/demo", "https://u:p@example.test", "https://a.test?token=x"])
def test_unsafe_base_urls_rejected(url):
    with pytest.raises(ValueError):
        AsyncGateway(api_key="key", base_url=url)

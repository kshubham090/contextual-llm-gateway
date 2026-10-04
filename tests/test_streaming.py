"""Real transport framing, cancellation, partial failure and durable SSE completion."""
import asyncio
import json
from contextlib import aclosing
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

from app.auth import Principal, authenticate
from app.chat_api import ManagedStreamingResponse
from app.chat_api import router as chat_router
from app.config import settings
from app.db import MemoryConflict
from app.main import create_app
from app.pipeline import Pipeline
from app.providers import (
    CompletionResult,
    OpenAICompatibleProvider,
    ProviderProtocolError,
    Router,
    StreamEvent,
)
from tests.test_pipeline import FakeDB, pipeline, req
from tests.test_providers import config


def provider_config(**overrides):
    return config(**{
        "generation_backend": "openai_compatible", "openai_base_url": "http://inference.test/v1",
        "openai_api_key": "", "openai_max_tokens_field": "max_completion_tokens",
        "openai_stream_usage": True, "provider_max_response_bytes": 8192, **overrides,
    })


def frame(text="", finish=None, usage=None):
    data = {"choices": [{"index": 0, "delta": {"content": text}, "finish_reason": finish}]}
    if usage is not None:
        data["usage"] = usage
    return ("data: " + json.dumps(data, ensure_ascii=False) + "\n\n").encode()


DONE = b"data: [DONE]\n\n"
TOKENS = {"prompt_tokens": 10, "completion_tokens": 2}


class WireStream(httpx.AsyncByteStream):
    def __init__(self, parts, gate=None, error=None):
        self.parts, self.gate, self.error = parts, gate, error
        self.closed = False
        self.waiting = asyncio.Event()

    async def __aiter__(self):
        for index, part in enumerate(self.parts):
            if index == 1 and self.gate is not None:
                self.waiting.set()
                await self.gate.wait()
            yield part
        if self.error:
            raise self.error

    async def aclose(self):
        self.closed = True


def make_router(handler, **overrides):
    cfg = provider_config(**overrides)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = OpenAICompatibleProvider(config=cfg, http_client=client)
    return Router(provider=provider, config=cfg)


async def test_real_wire_delta_arrives_before_final_and_unicode_survives_split_bytes():
    gate, requests = asyncio.Event(), []
    encoded = frame(" café")
    split = encoded.index("é".encode()) + 1
    wire = WireStream([frame("Hello"), encoded[:split], encoded[split:],
                       frame(finish="stop", usage=TOKENS), DONE], gate=gate)

    def handle(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, stream=wire)

    router = make_router(handle)
    try:
        async with aclosing(router.stream("hi", "system", 23,
                           history=[{"role": "user", "content": "past"},
                                    {"role": "assistant", "content": "reply"}])) as stream:
            first = await anext(stream)
            assert first.delta == "Hello" and first.result is None
            pending = asyncio.create_task(anext(stream))
            await wire.waiting.wait()
            assert not pending.done()  # First token did not wait for a complete answer.
            gate.set()
            assert (await pending).delta == " café"
            final = await anext(stream)
            assert final.result.text == "Hello café"
            assert (final.result.tokens_in, final.result.tokens_out) == (10, 2)
            assert final.result.usage_available
        assert wire.closed
        assert requests[0]["max_completion_tokens"] == 23
        assert requests[0]["stream_options"] == {"include_usage": True}
        assert [m["role"] for m in requests[0]["messages"]] == ["system", "user", "assistant", "user"]
    finally:
        await router.close()


@pytest.mark.parametrize("status", [429, 503])
async def test_stream_fallback_only_before_any_provider_text(status):
    models = []

    def handle(request):
        models.append(json.loads(request.content)["model"])
        if len(models) == 1:
            return httpx.Response(status)
        return httpx.Response(200, stream=WireStream([frame("fallback"), frame(finish="stop"), DONE]))

    router = make_router(handle)
    try:
        events = [event async for event in router.stream("hi", None, 10)]
        assert events[-1].result.fallback_used
        assert events[-1].result.text == "fallback"
        assert models == [settings.simple_model, settings.complex_model]
    finally:
        await router.close()


async def test_network_error_after_delta_never_falls_back_and_closes_transport():
    calls = []
    wire = WireStream([frame("partial")], error=httpx.ReadError("private upstream details"))

    def handle(request):
        calls.append(request)
        return httpx.Response(200, stream=wire)

    router = make_router(handle, provider_failure_threshold=1)
    try:
        async with aclosing(router.stream("hi", None, 10)) as stream:
            assert (await anext(stream)).delta == "partial"
            with pytest.raises(httpx.ReadError):
                await anext(stream)
        assert len(calls) == 1 and wire.closed
        assert settings.simple_model in router.health()["open_circuits"]
        assert router.health()["admitted_requests"] == 0
    finally:
        await router.close()


@pytest.mark.parametrize("parts", [
    [frame("partial"), frame(finish="stop")],  # Missing terminal marker.
    [frame("partial"), DONE],  # No finish reason.
    [frame("partial"), b'data: {"choices":'],  # Truncated event.
    [b'data: {"choices":[{"delta":{"tool_calls":[{}]}}]}\n\n', DONE],
    [frame("partial"), frame(finish="stop", usage={"prompt_tokens": -1, "completion_tokens": 2}), DONE],
])
async def test_incomplete_or_unsupported_stream_cannot_be_a_completed_answer(parts):
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, stream=WireStream(parts))

    router = make_router(handle)
    try:
        with pytest.raises(ProviderProtocolError):
            _ = [event async for event in router.stream("hi", None, 10)]
        assert len(calls) == 1
    finally:
        await router.close()


async def test_missing_provider_usage_is_explicit_unknown_not_a_free_generation():
    router = make_router(lambda request: httpx.Response(
        200, stream=WireStream([frame("answer"), frame(finish="stop"), DONE])))
    p = pipeline(router=router)
    try:
        events = [event async for event in p.stream_chat(req(), tenant_id="team-a")]
        meta = events[-1][1]["meta"]
        assert meta["usage_available"] is False and meta["cost"] is None
        assert p.db.logged[0]["cost"] is None
    finally:
        await router.close()


async def test_complete_compatible_payload_and_missing_usage():
    bodies = []

    def handle(request):
        bodies.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": "ok"},
                                                      "finish_reason": "length"}]})

    router = make_router(handle)
    try:
        result = await router.complete("hi", None, 10)
        assert result.text == "ok" and result.finish_reason == "length"
        assert not result.usage_available
        assert bodies[0]["stream"] is False and "stream_options" not in bodies[0]
    finally:
        await router.close()


async def test_cancel_waiting_on_provider_closes_http_stream_and_releases_admission():
    wire = WireStream([frame("one"), frame("two")], gate=asyncio.Event())
    router = make_router(lambda request: httpx.Response(200, stream=wire))
    try:
        stream = router.stream("hi", None, 10)
        assert (await anext(stream)).delta == "one"
        task = asyncio.create_task(anext(stream))
        await wire.waiting.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        await stream.aclose()
        assert wire.closed and router.health()["admitted_requests"] == 0
    finally:
        await router.close()


class DeltaRouter:
    """Deterministic provider events; transport-level authenticity is tested above."""
    def __init__(self, count=2, failure=None):
        self.count, self.failure = count, failure
        self.produced = 0
        self.closed = asyncio.Event()
        self.seen = None

    async def stream(self, prompt, system, max_tokens, **options):
        self.seen = (prompt, system, max_tokens, options)
        try:
            for _ in range(self.count):
                self.produced += 1
                yield StreamEvent(delta="x", model=settings.simple_model, provider="test")
            if self.failure:
                raise self.failure
            yield StreamEvent(result=CompletionResult(
                "x" * self.count, settings.simple_model, "test", 10, self.count, False, None))
        finally:
            self.closed.set()


async def test_stream_final_waits_for_database_commit_and_records_lineage():
    entered, commit = asyncio.Event(), asyncio.Event()

    class GatedDB(FakeDB):
        async def log_call(self, **kwargs):
            entered.set()
            await commit.wait()
            await super().log_call(**kwargs)

    p = pipeline(db=GatedDB(), router=DeltaRouter())
    async with aclosing(p.stream_chat(req(), tenant_id="team-a")) as stream:
        assert (await anext(stream))[0] == "delta"
        assert (await anext(stream))[0] == "delta"
        last = asyncio.create_task(anext(stream))
        await entered.wait()
        assert not last.done() and not p.db.logged
        commit.set()
        event, response = await last
        assert event == "final" and response["meta"]["durable"] is True
        assert p.db.logged[0]["expected_memory_epoch"] == 0
        assert p.db.logged[0]["source_ids"] == []


@pytest.mark.parametrize("failure", [httpx.ReadError("private upstream"), MemoryConflict("changed")])
async def test_failed_partial_stream_has_no_durable_completion_or_accounting(failure):
    class FailedCommit(FakeDB):
        async def log_call(self, **kwargs):
            if isinstance(failure, MemoryConflict):
                raise failure
            await super().log_call(**kwargs)

    provider = DeltaRouter(failure=failure if not isinstance(failure, MemoryConflict) else None)
    p = pipeline(db=FailedCommit(), router=provider)
    seen = []
    with pytest.raises(type(failure)):
        async for event in p.stream_chat(req(), tenant_id="team-a"):
            seen.append(event)
    assert seen and all(event == "delta" for event, _ in seen)
    assert not p.db.logged and provider.closed.is_set()


async def test_slow_consumer_backpressure_is_bounded_and_close_cancels_upstream():
    provider = DeltaRouter(count=1000)
    p = pipeline(router=provider)
    stream = p.stream_chat(req(), tenant_id="team-a")
    assert (await anext(stream))[0] == "delta"
    for _ in range(5):
        await asyncio.sleep(0)
    assert provider.produced <= 10  # one consumed + eight buffered + one awaiting queue space
    assert not p.db.logged
    await stream.aclose()
    assert provider.closed.is_set() and not p._streams
    assert p._capacity._value == settings.max_concurrent_requests


async def test_stalled_consumer_deadline_finishes_producer_without_terminal_queue_deadlock(monkeypatch):
    monkeypatch.setattr(settings, "request_timeout_seconds", 0.03)
    provider = DeltaRouter(count=1000)
    p = pipeline(router=provider)
    stream = p.stream_chat(req(), tenant_id="team-a")
    await anext(stream)
    await asyncio.wait_for(provider.closed.wait(), 1)
    await p.drain()  # Must not wait for the non-consuming client to drain an error queue.
    assert not p._streams and not p.db.logged
    with pytest.raises(TimeoutError):
        _ = [event async for event in stream]
    await stream.aclose()


def make_app(p):
    app = FastAPI()
    app.state.ready, app.state.pipeline = True, p
    app.dependency_overrides[authenticate] = lambda: Principal("team-a")
    app.include_router(chat_router)
    return app


def compatible_body(**changes):
    return {"model": "gateway-auto", "messages": [{"role": "user", "content": "Hi"}],
            "gateway": {"user_id": "u1", "feature_tag": "support"}, **changes}


def parse_sse(response):
    events = []
    for block in response.text.strip().split("\n\n"):
        event, payload = None, None
        for line in block.splitlines():
            if line.startswith("event: "):
                event = line[7:]
            if line.startswith("data: "):
                payload = line[6:]
        events.append((event, payload if payload == "[DONE]" else json.loads(payload)))
    return events


async def test_compatibility_stream_wire_has_stable_identity_usage_and_durable_finish():
    p = pipeline(router=DeltaRouter())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=make_app(p)), base_url="http://test") as c:
        response = await c.post("/v1/chat/completions", json=compatible_body(
            stream=True, stream_options={"include_usage": True}, max_completion_tokens=11,
            messages=[{"role": "system", "content": "Helpful"},
                      {"role": "user", "content": "Old question"},
                      {"role": "assistant", "content": "Old answer"},
                      {"role": "user", "content": "Hi"}]))
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    events = [data for _, data in parse_sse(response)]
    assert events[-1] == "[DONE]"
    chunks = events[:-1]
    assert len({chunk["id"] for chunk in chunks}) == len({chunk["created"] for chunk in chunks}) == 1
    assert all(chunk["object"] == "chat.completion.chunk" for chunk in chunks)
    assert chunks[0]["choices"][0]["delta"]["role"] == "assistant"
    assert "".join(chunk["choices"][0]["delta"].get("content", "")
                   for chunk in chunks if chunk["choices"]) == "xx"
    assert chunks[-2]["gateway"]["durable"]
    assert chunks[-2]["choices"][0]["finish_reason"] == "stop"
    assert chunks[-1]["choices"] == [] and chunks[-1]["usage"]["total_tokens"] == 12
    assert p.router.seen[1] == "Helpful" and p.router.seen[2] == 11
    assert p.router.seen[3]["history"][0]["content"] == "Old question"


@pytest.mark.parametrize("unsupported", [
    {"tools": []}, {"temperature": 0.2}, {"n": 2}, {"model": "not-configured"},
    {"max_tokens": 2, "max_completion_tokens": 2}, {"stream_options": {"include_usage": True}},
    {"messages": [{"role": "user", "content": [{"type": "text", "text": "Hi"}]}]},
    {"messages": [{"role": "assistant", "content": "Hi"}]},
    {"messages": [{"role": "user", "content": "Hi"}, {"role": "system", "content": "later"}]},
])
async def test_compatibility_rejects_unsupported_requests_before_generation(unsupported):
    p = pipeline(router=DeltaRouter())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=make_app(p)), base_url="http://test") as c:
        response = await c.post("/v1/chat/completions", json=compatible_body(**unsupported))
    assert response.status_code == 422
    assert p.router.seen is None and not p.db.logged


async def test_native_stream_error_redacts_details_and_never_sends_final():
    p = pipeline(router=DeltaRouter(failure=httpx.ReadError("private secret")))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=make_app(p)), base_url="http://test") as c:
        response = await c.post("/v1/chat/stream", json=req().model_dump())
    events = parse_sse(response)
    assert [event for event, _ in events] == ["delta", "delta", "error"]
    assert events[-1][1] == {"error": {"code": "inference_error", "message": "Inference service unavailable"},
                             "partial": True, "durable": False}
    assert "private secret" not in response.text and not p.db.logged


async def test_stream_memory_conflict_is_safe_error_after_deltas():
    class ConflictingDB(FakeDB):
        async def log_call(self, **kwargs):
            raise MemoryConflict("private memory changed")

    p = pipeline(db=ConflictingDB(), router=DeltaRouter())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=make_app(p)), base_url="http://test") as c:
        response = await c.post("/v1/chat/stream", json=req().model_dump())
    final_event, payload = parse_sse(response)[-1]
    assert final_event == "error" and payload["error"]["code"] == "memory_conflict"
    assert payload["durable"] is False and payload["partial"] is True
    assert not p.db.logged


async def test_failed_response_headers_close_prefetched_source_before_body_starts():
    closed = asyncio.Event()

    async def source():
        try:
            yield "one"
            await asyncio.Event().wait()
        finally:
            closed.set()

    stream = source()
    await anext(stream)

    async def body():
        yield "data: one\n\n"

    async def send(message):
        raise OSError("disconnected before headers")

    response = ManagedStreamingResponse(body(), source=stream, media_type="text/event-stream")
    with pytest.raises(Exception):
        await response({"type": "http", "asgi": {"spec_version": "2.4"}}, None, send)
    assert closed.is_set()
async def test_disconnect_before_first_provider_token_cancels_upstream(monkeypatch):
    key = 'review-only-' + 'x' * 40
    monkeypatch.setattr(settings, 'gateway_api_keys', {key: 'review'})
    monkeypatch.setattr(settings, 'auth_enabled', True)
    started, stopped, disconnect = asyncio.Event(), asyncio.Event(), asyncio.Event()
    class NeverRouter:
        async def stream(self, *args, **kwargs):
            started.set()
            try:
                await asyncio.Event().wait()
                yield StreamEvent(delta='never')
            finally:
                stopped.set()
    app = create_app()
    app.state.ready = True
    app.state.pipeline = Pipeline(
        SimpleNamespace(memory_epoch=AsyncMock(return_value=0)), None,
        SimpleNamespace(space_id='test'),
        SimpleNamespace(check=AsyncMock(return_value=(True, 0))), NeverRouter(),
    )
    body = json.dumps({'prompt':'hello','user_id':'u','feature_tag':'f','store':False,
                       'use_cache':False,'retrieval_mode':'none'}).encode()
    delivered = False
    async def receive():
        nonlocal delivered
        if not delivered:
            delivered = True
            return {'type':'http.request','body':body,'more_body':False}
        await disconnect.wait()
        return {'type':'http.disconnect'}
    async def send(message):
        pass
    scope = {'type':'http','asgi':{'version':'3.0','spec_version':'2.3'}, 'http_version':'1.1',
             'method':'POST','scheme':'http','path':'/v1/chat/stream','raw_path':b'/v1/chat/stream',
             'query_string':b'', 'root_path':'', 'server':('test',80),'client':('test',1),
             'headers':[(b'content-type',b'application/json'),(b'authorization',('Bearer '+key).encode())]}
    task = asyncio.create_task(app(scope, receive, send))
    try:
        await asyncio.wait_for(started.wait(), 1)
        disconnect.set()
        await asyncio.sleep(.1)
        assert stopped.is_set(), 'Disconnect before first delta leaves upstream generation running'
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_anthropic_sdk_stream_emits_provider_text_before_final_message():
    import anthropic

    from app.providers import AnthropicProvider

    def event(kind, **payload):
        return (f"event: {kind}\ndata: " + json.dumps({"type": kind, **payload}) + "\n\n").encode()

    gate = asyncio.Event()
    first = b"".join([
        event("message_start", message={"id": "msg_test", "type": "message", "role": "assistant",
              "content": [], "model": settings.simple_model, "stop_reason": None, "stop_sequence": None,
              "usage": {"input_tokens": 10, "output_tokens": 0}}),
        event("content_block_start", index=0, content_block={"type": "text", "text": ""}),
        event("content_block_delta", index=0, delta={"type": "text_delta", "text": "SDK delta"}),
    ])
    rest = b"".join([
        event("content_block_stop", index=0),
        event("message_delta", delta={"stop_reason": "end_turn", "stop_sequence": None},
              usage={"output_tokens": 2}),
        event("message_stop"),
    ])
    wire = WireStream([first, rest], gate=gate)
    provider = AnthropicProvider(config=config())
    await provider.client.close()
    provider.client = anthropic.AsyncAnthropic(api_key="unused", max_retries=0,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=wire))))
    router = Router(provider=provider, config=config())
    try:
        async with aclosing(router.stream("hi", None, 10)) as stream:
            assert (await anext(stream)).delta == "SDK delta"
            final = asyncio.create_task(anext(stream))
            await wire.waiting.wait()
            assert not final.done()
            gate.set()
            result = (await final).result
            assert result.text == "SDK delta" and (result.tokens_in, result.tokens_out) == (10, 2)
        assert wire.closed
    finally:
        await router.close()


async def test_stream_attempt_deadline_after_text_closes_upstream_without_fallback():
    wire = WireStream([frame("one"), frame("never")], gate=asyncio.Event())
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, stream=wire)

    router = make_router(handle, provider_timeout_seconds=0.03)
    try:
        async with aclosing(router.stream("hi", None, 10)) as stream:
            assert (await anext(stream)).delta == "one"
            with pytest.raises(TimeoutError):
                await anext(stream)
        assert wire.closed and len(calls) == 1
    finally:
        await router.close()


async def test_native_cached_stream_is_one_explicit_cache_delta_and_durable_final():
    from tests.test_pipeline import similar

    p = pipeline(db=FakeDB([similar()]), router=DeltaRouter())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=make_app(p)), base_url="http://test") as c:
        response = await c.post("/v1/chat/stream", json=req().model_dump())
    events = parse_sse(response)
    assert [kind for kind, _ in events] == ["delta", "final"]
    assert events[0][1]["delta"] == "cached answer"
    assert events[1][1]["meta"]["cache_hit"] and events[1][1]["meta"]["durable"]
    assert p.router.produced == 0 and not p.embedder.requests


async def test_nonstream_adapter_keeps_full_answer_and_gateway_metadata():
    p = pipeline()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=make_app(p)), base_url="http://test") as c:
        response = await c.post("/v1/chat/completions", json=compatible_body())
    data = response.json()
    assert response.status_code == 200 and data["object"] == "chat.completion"
    assert data["choices"][0]["message"] == {"role": "assistant", "content": "llm answer"}
    assert data["usage"]["total_tokens"] == 150 and data["gateway"]["durable"]


@pytest.mark.parametrize("streaming", [False, True])
async def test_provider_response_byte_limit_closes_connection_and_prevents_persistence(streaming):
    wire = WireStream([frame("x" * 2000), frame(finish="stop"), DONE] if streaming else [
        json.dumps({"choices": [{"message": {"content": "x" * 2000}, "finish_reason": "stop"}]}).encode()
    ])
    router = make_router(lambda request: httpx.Response(200, stream=wire), provider_max_response_bytes=1024)
    try:
        with pytest.raises(ProviderProtocolError, match="byte limit"):
            if streaming:
                _ = [event async for event in router.stream("hi", None, 10)]
            else:
                await router.complete("hi", None, 10)
        assert wire.closed and router.health()["admitted_requests"] == 0
    finally:
        await router.close()

"""Native SSE and an explicitly limited, text-only Chat Completions adapter."""
from __future__ import annotations

import asyncio
import json
import time
import uuid
from contextlib import aclosing
from typing import Literal

import anthropic
import anyio
import asyncpg
import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator
from redis.exceptions import RedisError

from .auth import Principal, authenticate
from .config import settings
from .db import MemoryConflict
from .embeddings import EmbeddingClosedError, EmbeddingError, EmbeddingOverloadedError
from .pipeline import GatewayOverloaded, RateLimitExceeded
from .providers import (
    CircuitOpenError,
    ProviderClosedError,
    ProviderOverloadedError,
    ProviderProtocolError,
    choose_model,
)
from .schemas import ChatMessage, ChatRequest

router = APIRouter()


class ManagedStreamingResponse(StreamingResponse):
    """Close the prefetched source even when headers or a body send fail."""

    def __init__(self, *args, source, **kwargs):
        super().__init__(*args, **kwargs)
        self._source = source

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            with anyio.CancelScope(shield=True):
                await self.body_iterator.aclose()
                await self._source.aclose()


def classify_error(exc: Exception) -> tuple[int, str, str]:
    if isinstance(exc, MemoryConflict):
        return 409, "memory_conflict", "Memory changed during this request; retry with the current memory"
    if isinstance(exc, RateLimitExceeded):
        return 429, "rate_limit_exceeded", "Rate limit exceeded"
    if isinstance(exc, (GatewayOverloaded, EmbeddingOverloadedError, ProviderOverloadedError,
                        CircuitOpenError,
                        ProviderClosedError, EmbeddingClosedError)):
        return 503, "capacity_unavailable", "Gateway capacity temporarily unavailable"
    if isinstance(exc, TimeoutError):
        return 504, "deadline_exceeded", "Request deadline exceeded"
    if isinstance(exc, (EmbeddingError, ProviderProtocolError, httpx.HTTPError, anthropic.APIError)):
        return 502, "inference_error", "Inference service unavailable"
    if isinstance(exc, (RedisError, asyncpg.PostgresError, ConnectionError)):
        return 503, "storage_unavailable", "Required storage unavailable"
    if isinstance(exc, ValueError):
        return 422, "unsupported_request", "Request is outside the supported text-generation contract"
    return 503, "service_unavailable", "Service temporarily unavailable"


def raise_http_error(exc):
    status, _, message = classify_error(exc)
    headers = {"Retry-After": str(exc.retry_after)} if isinstance(exc, RateLimitExceeded) else None
    raise HTTPException(status, message, headers=headers) from exc


def sse(data, event=None):
    prefix = f"event: {event}\n" if event else ""
    payload = data if isinstance(data, str) else json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    return prefix + "data: " + payload + "\n\n"


async def first_event(request, native, principal):
    if not request.app.state.ready:
        raise HTTPException(503, "Gateway is not ready", headers={"Retry-After": "1"})
    stream = request.app.state.pipeline.stream_chat(native, tenant_id=principal.tenant_id)

    async def disconnected():
        # FastAPI has already read/validated the complete JSON body. During
        # prefetch there is no StreamingResponse to listen for disconnects yet.
        while True:
            if (await request.receive())["type"] == "http.disconnect":
                return

    pending = asyncio.create_task(anext(stream), name="gateway-stream-prefetch")
    monitor = asyncio.create_task(disconnected(), name="gateway-stream-disconnect")
    handed_off = False
    try:
        await asyncio.wait((pending, monitor), return_when=asyncio.FIRST_COMPLETED)
        # Stop receive ownership before StreamingResponse takes it. Check after
        # joining too, so a disconnect concurrent with the first delta is kept.
        monitor.cancel()
        await asyncio.gather(monitor, return_exceptions=True)
        if not monitor.cancelled():
            monitor.result()
            raise HTTPException(499, "Client disconnected")
        first = pending.result()
        handed_off = True
        return stream, first
    except HTTPException:
        raise
    except Exception as exc:
        raise_http_error(exc)
    finally:
        with anyio.CancelScope(shield=True):
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)
            if not handed_off:
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
                await stream.aclose()


@router.post("/v1/chat/stream")
async def native_stream(native: ChatRequest, request: Request, principal: Principal = Depends(authenticate)):
    stream, first = await first_event(request, native, principal)

    async def body():
        partial = False
        try:
            async with aclosing(stream):
                event, data = first
                partial = event == "delta" and bool(data.get("delta"))
                yield sse(data, event)
                async for event, data in stream:
                    partial = partial or (event == "delta" and bool(data.get("delta")))
                    yield sse(data, event)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _, code, message = classify_error(exc)
            yield sse({"error": {"code": code, "message": message},
                       "partial": partial, "durable": False}, "error")

    return ManagedStreamingResponse(body(), source=stream, media_type="text/event-stream",
                             headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})


class CompatibilityMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["system", "user", "assistant"]
    content: str = Field(min_length=1, max_length=64000)


class GatewayOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")
    user_id: str = Field(min_length=1, max_length=128, pattern=r"^[\w.@:-]+$")
    feature_tag: str = Field(min_length=1, max_length=128, pattern=r"^[\w.@:-]+$")
    retrieval_mode: Literal["none", "vector", "graph"] = "graph"
    use_cache: bool = True
    cache_mode: Literal["exact", "semantic"] = "exact"
    store: bool = True


class StreamOptions(BaseModel):
    model_config = ConfigDict(extra="forbid")
    include_usage: bool = False


class CompatibilityRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    model: str = Field(min_length=1, max_length=256)
    messages: list[CompatibilityMessage] = Field(min_length=1, max_length=34)
    gateway: GatewayOptions
    max_tokens: int | None = Field(default=None, ge=1, le=8192)
    max_completion_tokens: int | None = Field(default=None, ge=1, le=8192)
    stream: bool = False
    stream_options: StreamOptions | None = None
    n: Literal[1] = 1

    @model_validator(mode="after")
    def supported_subset(self):
        if self.max_tokens is not None and self.max_completion_tokens is not None:
            raise ValueError("Specify max_completion_tokens or max_tokens, not both")
        if self.stream_options is not None and not self.stream:
            raise ValueError("stream_options requires stream=true")
        if self.model not in {"gateway-auto", settings.simple_model, settings.complex_model}:
            raise ValueError("Model must be gateway-auto or one of the configured routing models")
        self.native_request()  # Also validate message ordering and total character budget.
        return self

    def native_request(self) -> ChatRequest:
        messages = list(self.messages)
        system = messages.pop(0).content if messages[0].role == "system" else None
        if not messages or messages[-1].role != "user" or any(m.role == "system" for m in messages):
            raise ValueError("Support one optional leading system message and a final user message")
        return ChatRequest(
            prompt=messages[-1].content, system_prompt=system,
            history=[ChatMessage(role=m.role, content=m.content) for m in messages[:-1]],
            model=None if self.model == "gateway-auto" else self.model,
            max_tokens=self.max_completion_tokens or self.max_tokens, **self.gateway.model_dump(),
        )


def usage(metadata):
    if not metadata.get("usage_available", True):
        return None
    incoming, outgoing = metadata["tokens_in"], metadata["tokens_out"]
    return {"prompt_tokens": incoming, "completion_tokens": outgoing, "total_tokens": incoming + outgoing}


@router.post("/v1/chat/completions")
async def completions(body: CompatibilityRequest, request: Request,
                      principal: Principal = Depends(authenticate)):
    native = body.native_request()
    response_id, created = "chatcmpl-" + uuid.uuid4().hex, int(time.time())
    selected_model = native.model or choose_model(native.prompt)
    if not request.app.state.ready:
        raise HTTPException(503, "Gateway is not ready")
    if not body.stream:
        try:
            response = await request.app.state.pipeline.handle_chat(native, tenant_id=principal.tenant_id)
        except Exception as exc:
            raise_http_error(exc)
        metadata = response.meta.model_dump()
        return {
            "id": response_id, "object": "chat.completion", "created": created,
            "model": metadata["model"] or selected_model,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": response.response},
                         "finish_reason": metadata["finish_reason"]}],
            "usage": usage(metadata), "gateway": metadata,
        }

    stream, first = await first_event(request, native, principal)

    async def events():
        partial = False
        model = (first[1].get("model") if first[0] == "delta" else None) or selected_model

        def chunk(choices, **extra):
            return {"id": response_id, "object": "chat.completion.chunk", "created": created,
                    "model": model, "choices": choices, **extra}

        try:
            async with aclosing(stream):
                yield sse(chunk([{"index": 0, "delta": {"role": "assistant", "content": ""},
                                  "finish_reason": None}]))
                current = first
                while True:
                    event, data = current
                    if event == "delta":
                        partial = partial or bool(data["delta"])
                        yield sse(chunk([{"index": 0, "delta": {"content": data["delta"]},
                                          "finish_reason": None}]))
                    elif event == "final":
                        metadata = data["meta"]
                        yield sse(chunk([{"index": 0, "delta": {},
                                          "finish_reason": metadata["finish_reason"]}], gateway=metadata))
                        if body.stream_options and body.stream_options.include_usage:
                            yield sse(chunk([], usage=usage(metadata)))
                        yield sse("[DONE]")
                        return
                    try:
                        current = await anext(stream)
                    except StopAsyncIteration:
                        raise RuntimeError("Gateway stream ended before durable completion") from None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _, code, message = classify_error(exc)
            yield sse({"error": {"code": code, "message": message, "type": "gateway_error"},
                       "gateway": {"partial": partial, "durable": False}})
            yield sse("[DONE]")

    return ManagedStreamingResponse(events(), source=stream, media_type="text/event-stream",
                             headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"})

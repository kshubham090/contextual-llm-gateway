"""No automatic retries: a failed write may already have incurred a provider charge."""

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast
from urllib.parse import quote, urlsplit

import httpx

from .types import (
    ChatRequest,
    ChatResponse,
    Memory,
    MemoryDeletion,
    MemoryMutation,
    MemoryPage,
    StreamEvent,
)

_MAX_EVENT_CHARS = 2 * 1024 * 1024


class GatewayError(Exception):
    """An HTTP error from the gateway; details retain structured validation errors."""

    def __init__(self, message: str, *, status_code: int, request_id: str | None = None,
                 retry_after: str | None = None, details: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.request_id = request_id
        self.retry_after = retry_after
        self.details = details


class GatewayTransportError(Exception):
    """Connection/read failure. Delivery is uncertain; the client does not retry."""


class GatewayProtocolError(Exception):
    """The server returned malformed JSON, an invalid event, or an incomplete stream."""


class GatewayStreamError(Exception):
    """The gateway failed after opening an SSE stream; partial text is not a success."""

    def __init__(self, message: str, *, code: str, partial: bool,
                 request_id: str | None = None):
        super().__init__(message)
        self.code = code
        self.partial = partial
        self.durable = False
        self.request_id = request_id


def _json_object(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise GatewayProtocolError("Expected a JSON object from the gateway")
    return value


def _chat_response(value: Any) -> ChatResponse:
    value = _json_object(value)
    if not isinstance(value.get("response"), str) or not isinstance(value.get("meta"), dict):
        raise GatewayProtocolError("Invalid chat response from the gateway")
    return cast(ChatResponse, value)


def _scope(user_id: str, feature_tag: str) -> dict[str, str]:
    if not user_id or not feature_tag:
        raise ValueError("user_id and feature_tag are required")
    return {"user_id": user_id, "feature_tag": feature_tag}


class AsyncGateway:
    """Keep tenant credentials on a trusted server. Reuse one client per application.

    The client owns its httpx connection pool; use ``async with`` or call ``aclose``.
    A custom transport supports deterministic tests without provider credentials.
    """

    def __init__(self, *, api_key: str, base_url: str = "http://127.0.0.1:8000",
                 timeout: float | httpx.Timeout = 120.0,
                 transport: httpx.AsyncBaseTransport | None = None):
        url = urlsplit(base_url)
        if url.scheme not in {"http", "https"} or not url.netloc:
            raise ValueError("base_url must be an absolute HTTP(S) URL")
        if url.username or url.password or url.query or url.fragment:
            raise ValueError("base_url cannot contain credentials, a query, or a fragment")
        if not api_key or "\r" in api_key or "\n" in api_key:
            raise ValueError("api_key must be a nonempty bearer token")
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/") + "/",
            headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
            timeout=timeout,
            transport=transport,
            follow_redirects=False,
        )

    async def __aenter__(self) -> "AsyncGateway":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    @staticmethod
    def _check_status(response: httpx.Response) -> None:
        if 200 <= response.status_code < 300:
            return
        try:
            details = response.json()
        except ValueError:
            details = None
        detail = details.get("detail") if isinstance(details, dict) else None
        message = detail if isinstance(detail, str) else f"Gateway returned HTTP {response.status_code}"
        raise GatewayError(
            message, status_code=response.status_code,
            request_id=response.headers.get("x-request-id"),
            retry_after=response.headers.get("retry-after"), details=details,
        )

    async def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        try:
            response = await self._client.request(method, path, **kwargs)
            self._check_status(response)
            try:
                return _json_object(response.json())
            except ValueError as exc:
                raise GatewayProtocolError("Invalid JSON from the gateway") from exc
        except httpx.HTTPError as exc:
            raise GatewayTransportError(
                "Gateway request transport failed; delivery may be uncertain"
            ) from exc

    async def chat(self, request: ChatRequest) -> ChatResponse:
        """Send one native chat request. No retries are performed."""
        _scope(request.get("user_id", ""), request.get("feature_tag", ""))
        return _chat_response(await self._request("POST", "v1/chat", json=request))

    @asynccontextmanager
    async def stream_chat(self, request: ChatRequest) -> AsyncIterator[AsyncIterator[StreamEvent]]:
        """Yield delta/final events; context exit closes the stream even after an early break.

        A final event confirms the server committed accounting/memory. Cancellation or
        an error before final leaves any partial text unconfirmed. Cancel the consuming
        asyncio task to abort a pending read.
        """
        _scope(request.get("user_id", ""), request.get("feature_tag", ""))
        try:
            async with self._client.stream(
                "POST", "v1/chat/stream", json=request,
                headers={"Accept": "text/event-stream"},
            ) as response:
                if not 200 <= response.status_code < 300:
                    await response.aread()
                    self._check_status(response)
                if response.headers.get("content-type", "").split(";", 1)[0].strip() != "text/event-stream":
                    raise GatewayProtocolError("Expected text/event-stream from the gateway")
                yield self._events(response)
        except httpx.HTTPError as exc:
            raise GatewayTransportError("Gateway stream transport failed; delivery may be uncertain") from exc

    async def _events(self, response: httpx.Response) -> AsyncIterator[StreamEvent]:
        event = ""
        data: list[str] = []
        size = 0
        first_line = True
        async for line in response.aiter_lines():
            if first_line:
                line = line.removeprefix("\ufeff")
                first_line = False
            size += len(line)
            if size > _MAX_EVENT_CHARS:
                raise GatewayProtocolError("Gateway SSE event exceeds the client size limit")
            if line == "":
                if data and event in {"delta", "final", "error"}:
                    try:
                        value = _json_object(json.loads("\n".join(data)))
                    except ValueError as exc:
                        raise GatewayProtocolError("Invalid JSON in gateway SSE event") from exc
                    if event == "error":
                        error = _json_object(value.get("error"))
                        if not all(isinstance(error.get(key), str) for key in ("message", "code")):
                            raise GatewayProtocolError("Invalid gateway SSE error")
                        raise GatewayStreamError(
                            error["message"], code=error["code"], partial=bool(value.get("partial")),
                            request_id=response.headers.get("x-request-id"),
                        )
                    if event == "delta":
                        if not isinstance(value.get("delta"), str):
                            raise GatewayProtocolError("Invalid gateway text delta")
                        yield cast(StreamEvent, {**value, "type": "delta"})
                    else:
                        final = _chat_response(value)
                        if final["meta"].get("durable") is not True:
                            raise GatewayProtocolError(
                                "Gateway final event did not confirm durable completion"
                            )
                        yield {"type": "final", "response": final}
                        return
                event, data, size = "", [], 0
            elif not line.startswith(":"):
                field, sep, value = line.partition(":")
                if sep and value.startswith(" "):
                    value = value[1:]
                if field == "event":
                    event = value
                elif field == "data":
                    data.append(value)
        raise GatewayProtocolError("Gateway stream ended before a final event")

    async def list_memories(self, *, user_id: str, feature_tag: str,
                            limit: int = 50, cursor: str | None = None) -> MemoryPage:
        params: dict[str, Any] = {**_scope(user_id, feature_tag), "limit": limit}
        if cursor is not None:
            params["cursor"] = cursor
        return cast(MemoryPage, await self._request("GET", "v1/memories", params=params))

    async def get_memory(self, memory_id: str, *, user_id: str, feature_tag: str) -> Memory:
        return cast(Memory, await self._request(
            "GET", f"v1/memories/{quote(memory_id, safe='')}", params=_scope(user_id, feature_tag),
        ))

    async def create_memory(self, *, user_id: str, feature_tag: str, prompt: str,
                            response: str, expected_scope_revision: int) -> MemoryMutation:
        return cast(MemoryMutation, await self._request("POST", "v1/memories", json={
            **_scope(user_id, feature_tag), "prompt": prompt, "response": response,
            "expected_scope_revision": expected_scope_revision,
        }))

    async def correct_memory(self, memory_id: str, *, user_id: str, feature_tag: str,
                             prompt: str, response: str, expected_revision: int) -> MemoryMutation:
        return cast(MemoryMutation, await self._request(
            "PATCH", f"v1/memories/{quote(memory_id, safe='')}", json={
                **_scope(user_id, feature_tag), "prompt": prompt, "response": response,
                "expected_revision": expected_revision,
            },
        ))

    async def delete_memory(self, memory_id: str, *, user_id: str, feature_tag: str,
                            expected_revision: int) -> MemoryDeletion:
        return cast(MemoryDeletion, await self._request(
            "DELETE", f"v1/memories/{quote(memory_id, safe='')}", params={
                **_scope(user_id, feature_tag), "expected_revision": expected_revision,
            },
        ))

    async def delete_scope(self, *, user_id: str, feature_tag: str,
                           expected_scope_revision: int) -> MemoryDeletion:
        """Delete all memory in this exact scope with an optimistic revision guard."""
        return cast(MemoryDeletion, await self._request("DELETE", "v1/memories", params={
            **_scope(user_id, feature_tag), "expected_scope_revision": expected_scope_revision,
        }))

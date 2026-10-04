"""Enforce limits on actual received bytes, including chunked requests."""

import asyncio

from starlette.responses import JSONResponse

from .config import settings


class RequestSizeLimitMiddleware:
    def __init__(self, app) -> None:
        self.app = app
        self._limit = settings.max_concurrent_requests * 2
        self._slots = asyncio.Semaphore(self._limit)
        self._waiting = 0

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        if scope.get("path") in {"/health", "/health/live"}:
            return await self.app(scope, receive, send)
        if self._waiting >= self._limit:
            return await self._busy(scope, receive, send)
        self._waiting += 1
        try:
            await asyncio.wait_for(self._slots.acquire(), settings.admission_timeout_seconds)
        except TimeoutError:
            return await self._busy(scope, receive, send)
        finally:
            self._waiting -= 1
        try:
            return await self._handle(scope, receive, send)
        finally:
            self._slots.release()

    async def _busy(self, scope, receive, send):
        await JSONResponse(
            {"detail": "Gateway ingress capacity unavailable"}, 503, headers={"Retry-After": "1"}
        )(scope, receive, send)

    async def _handle(self, scope, receive, send):
        headers = dict(scope.get("headers", []))
        try:
            declared = int(headers.get(b"content-length", b"0"))
            if declared < 0:
                raise ValueError()
        except ValueError:
            return await JSONResponse({"detail": "Invalid Content-Length"}, 400)(scope, receive, send)
        if declared > settings.request_max_bytes:
            return await JSONResponse({"detail": "Request body too large"}, 413)(scope, receive, send)
        body = bytearray()
        try:
            async with asyncio.timeout(settings.request_body_timeout_seconds):
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        return
                    chunk = message.get("body", b"")
                    if len(body) + len(chunk) > settings.request_max_bytes:
                        return await JSONResponse({"detail": "Request body too large"}, 413)(
                            scope,
                            receive,
                            send,
                        )
                    body.extend(chunk)
                    if not message.get("more_body", False):
                        break
        except TimeoutError:
            return await JSONResponse({"detail": "Request body timed out"}, 408)(scope, receive, send)
        delivered = False

        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.app(scope, replay, send)

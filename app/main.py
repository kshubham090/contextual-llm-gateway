"""Authenticated API and explicit startup/shutdown of service-owned resources."""

import asyncio
import logging
from contextlib import asynccontextmanager
from time import perf_counter

import anthropic
import asyncpg
import httpx
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST
from redis.exceptions import RedisError

from . import metrics
from .auth import Principal, authenticate, authenticate_metrics
from .chat_api import router as chat_router
from .config import settings
from .console_api import router as console_router
from .db import Database, MemoryConflict
from .embeddings import EmbeddingClient, EmbeddingClosedError, EmbeddingError, EmbeddingOverloadedError
from .graph import MemoryGraph
from .inspector import router as inspector_router
from .logs import log, new_request_id, request_id_var, setup_logging
from .memory_api import router as memory_router
from .middleware import RequestSizeLimitMiddleware
from .outbox import GraphOutboxWorker
from .pipeline import GatewayOverloaded, Pipeline, RateLimitExceeded
from .providers import (
    CircuitOpenError,
    ProviderClosedError,
    ProviderOverloadedError,
    ProviderProtocolError,
    Router,
)
from .rate_limit import RateLimiter
from .schemas import ChatRequest, ChatResponse, UsageRow

logger = logging.getLogger("gateway.http")


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    settings.validate_runtime()
    resources = []
    worker = None
    pipeline = None
    app.state.ready = False
    try:
        db, graph, limiter = Database(), MemoryGraph(), RateLimiter()
        resources.extend([db, graph, limiter])
        embedder, router = EmbeddingClient(), Router()
        resources.extend([embedder, router])
        app.state.db, app.state.graph, app.state.limiter = db, graph, limiter
        app.state.embedder, app.state.router = embedder, router
        app.state.pipeline = Pipeline(db, graph, embedder, limiter, router)
        pipeline = app.state.pipeline
        await db.connect()
        await graph.connect()
        await limiter.connect()
        await embedder.start()
        worker = GraphOutboxWorker(db, graph)
        app.state.outbox = worker
        worker.start()
        app.state.ready = True
        yield
    finally:
        app.state.ready = False
        if pipeline is not None:
            await pipeline.close()
        if worker is not None:
            await worker.stop(timeout=min(5, settings.shutdown_timeout_seconds))
        if resources:
            try:
                async with asyncio.timeout(settings.shutdown_timeout_seconds):
                    results = await asyncio.gather(
                        *(resource.close() for resource in resources), return_exceptions=True
                    )
                    for result in results:
                        if isinstance(result, BaseException):
                            log(logger, "resource_close_failed", error_type=type(result).__name__)
            except TimeoutError:
                log(logger, "resource_close_timeout")


def create_app(*, lifespan_handler=lifespan) -> FastAPI:
    app = FastAPI(
        title="Contextual LLM Gateway",
        version="0.3.0",
        description="Scoped graph memory and measurable inference for trusted applications.",
        lifespan=lifespan_handler,
        docs_url="/docs" if settings.environment == "development" else None,
        redoc_url=None,
        openapi_url="/openapi.json" if settings.environment == "development" else None,
    )
    app.state.ready = False
    app.add_middleware(RequestSizeLimitMiddleware)
    app.include_router(inspector_router)
    app.include_router(chat_router)
    app.include_router(memory_router)
    app.include_router(console_router)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        previous = request_id_var.get()
        rid = new_request_id()
        start = perf_counter()
        try:
            try:
                response = await call_next(request)
            except Exception as exc:
                # Never return exception text: SDK/database errors can contain content or credentials.
                log(logger, "request_failed", error_type=type(exc).__name__)
                response = JSONResponse({"detail": "Service temporarily unavailable"}, status_code=503)
            response.headers["X-Request-ID"] = rid
            response.headers["Cache-Control"] = "no-store"
            route = request.scope.get("route")
            path = getattr(route, "path", "unmatched")
            duration = perf_counter() - start
            metrics.http_requests.labels(route=path, status=str(response.status_code)).inc()
            metrics.http_latency.labels(route=path).observe(duration)
            log(
                logger,
                "request",
                method=request.method,
                route=path,
                status=response.status_code,
                duration_ms=int(duration * 1000),
            )
            return response
        finally:
            request_id_var.set(previous)

    @app.post("/v1/chat", response_model=ChatResponse)
    async def chat(req: ChatRequest, request: Request, principal: Principal = Depends(authenticate)):
        if not app.state.ready:
            raise HTTPException(503, "Gateway is not ready", headers={"Retry-After": "1"})
        try:
            return await request.app.state.pipeline.handle_chat(req, tenant_id=principal.tenant_id)
        except MemoryConflict:
            raise HTTPException(409, "Memory changed during this request; retry with the current memory")
        except RateLimitExceeded as exc:
            raise HTTPException(429, "Rate limit exceeded", headers={"Retry-After": str(exc.retry_after)})
        except (
            GatewayOverloaded,
            EmbeddingOverloadedError,
            ProviderOverloadedError,
            CircuitOpenError,
            ProviderClosedError,
            EmbeddingClosedError,
        ):
            raise HTTPException(503, "Gateway capacity temporarily unavailable", headers={"Retry-After": "1"})
        except TimeoutError:
            raise HTTPException(504, "Request deadline exceeded")
        except (EmbeddingError, ProviderProtocolError, httpx.HTTPError, anthropic.APIError):
            raise HTTPException(502, "Inference service unavailable")
        except (RedisError, asyncpg.PostgresError, ConnectionError):
            raise HTTPException(503, "Required storage unavailable", headers={"Retry-After": "1"})
        except ValueError:
            raise HTTPException(422, "Request is outside the supported text-generation contract")

    @app.get("/v1/usage", response_model=list[UsageRow])
    async def usage(
        request: Request,
        principal: Principal = Depends(authenticate),
        user_id: str | None = Query(default=None, min_length=1, max_length=128),
        feature_tag: str | None = Query(default=None, min_length=1, max_length=128),
        limit: int = Query(default=100, ge=1, le=1000),
    ):
        return await request.app.state.db.usage_rollup(
            user_id=user_id,
            feature_tag=feature_tag,
            tenant_id=principal.tenant_id,
            limit=limit,
        )

    @app.get("/v1/graph/stats")
    async def graph_stats(
        request: Request,
        principal: Principal = Depends(authenticate),
        user_id: str | None = Query(default=None, min_length=1, max_length=128),
        feature_tag: str | None = Query(default=None, min_length=1, max_length=128),
    ):
        return await request.app.state.graph.stats(
            tenant_id=principal.tenant_id,
            user_id=user_id,
            feature_tag=feature_tag,
        )

    @app.get("/health", include_in_schema=False)
    @app.get("/health/live")
    async def health():
        return {"status": "ok"}

    @app.get("/health/ready")
    async def readiness(request: Request):
        if not request.app.state.ready:
            return JSONResponse({"status": "unavailable"}, status_code=503)
        state = request.app.state

        async def probe(resource):
            try:
                async with asyncio.timeout(2):
                    return bool(await resource.health())
            except Exception:
                return False

        postgres, redis, graph = await asyncio.gather(
            probe(state.db), probe(state.limiter), probe(state.graph)
        )
        embedding = state.embedder.health()
        ready = postgres and redis and embedding["started"] and not embedding["closed"]
        ready = ready and state.outbox.health()["running"]
        return JSONResponse(
            {
                "status": ("ready" if graph else "degraded") if ready else "unavailable",
                "dependencies": {"postgres": postgres, "redis": redis, "graph": graph},
            },
            status_code=200 if ready else 503,
        )

    @app.get("/metrics", dependencies=[Depends(authenticate_metrics)], include_in_schema=False)
    async def prometheus_metrics():
        metrics.observe_runtime(app.state)
        return Response(content=metrics.render(), headers={"Content-Type": CONTENT_TYPE_LATEST})

    return app


app = create_app()

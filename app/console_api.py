"""Authenticated console data from scoped accounting and authoritative memory."""

import asyncio
import re

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.routing import APIRoute

from .auth import Principal, authenticate
from .config import settings
from .db import MemoryConflict
from .memory_api import SCOPE_PATTERN, admit, scope


class ConsoleRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()

        async def handler(request: Request):
            try:
                async with asyncio.timeout(settings.request_timeout_seconds):
                    return await original(request)
            except MemoryConflict:
                raise HTTPException(409, "Memory changed; refresh the console")
            except TimeoutError:
                raise HTTPException(504, "Console operation deadline exceeded")

        return handler


router = APIRouter(prefix="/v1/console", tags=["console"], route_class=ConsoleRoute)


def model_label(value: str) -> str:
    # Model IDs are useful; configured local paths and service URLs are not.
    if (
        not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}", value)
        or "://" in value
        or re.match(r"^[A-Za-z]:/", value)
    ):
        return "configured model"
    return value


def runtime_metadata(request: Request) -> dict:
    state = request.app.state
    generation = getattr(state.router, "_config", settings)
    embedding = getattr(state.embedder, "_config", settings)
    synthetic_generation = getattr(state.router.provider, "name", "") == "synthetic-demo"
    synthetic_embedding = getattr(state.embedder, "space_id", "").startswith("synthetic-fixture:")
    return {
        "generation": {
            "backend": "synthetic-demo" if synthetic_generation else generation.generation_backend,
            "simple_model": "synthetic-demo"
            if synthetic_generation
            else model_label(generation.simple_model),
            "complex_model": "synthetic-demo"
            if synthetic_generation
            else model_label(generation.complex_model),
            "max_concurrency": generation.provider_max_concurrency,
            "default_max_tokens": generation.default_max_tokens,
        },
        "embedding": {
            "backend": "synthetic-fixture" if synthetic_embedding else embedding.embedding_backend,
            "model": "synthetic-fixture"
            if synthetic_embedding
            else model_label(
                embedding.local_embedding_model
                if embedding.embedding_backend == "local"
                else embedding.embedding_model
            ),
            "device": "cpu"
            if synthetic_embedding
            else (embedding.local_embedding_device if embedding.embedding_backend == "local" else "remote"),
            "dimensions": embedding.embedding_dim,
            "batch_size": embedding.embedding_batch_size,
            "workers": embedding.embedding_workers,
            "queue_size": embedding.embedding_queue_size,
        },
        "requests": {
            "max_concurrent": settings.max_concurrent_requests,
            "timeout_seconds": settings.request_timeout_seconds,
        },
        "docs_available": request.app.docs_url is not None,
    }


async def healthy(resource) -> bool:
    try:
        async with asyncio.timeout(min(2, settings.graph_timeout_seconds, settings.redis_timeout_seconds)):
            return bool(await resource.health())
    except Exception:
        return False


@router.get("/overview")
async def overview(
    request: Request,
    principal: Principal = Depends(authenticate),
    user_id: str = Query(min_length=1, max_length=128, pattern=SCOPE_PATTERN),
    feature_tag: str = Query(min_length=1, max_length=128, pattern=SCOPE_PATTERN),
):
    await admit(request, principal, user_id)
    state = request.app.state
    result = await state.db.console_overview(**scope(principal, user_id, feature_tag))
    redis_ok, graph_ok = await asyncio.gather(healthy(state.limiter), healthy(state.graph))
    embedding = state.embedder.health()
    result.update(
        {
            "runtime": runtime_metadata(request),
            "health": {
                "postgres": True,
                "redis": redis_ok,
                "graph": graph_ok,
                "embedding": bool(embedding.get("started") and not embedding.get("closed")),
            },
            "limitations": [
                "Accounting covers recorded completions, including private requests; "
                "failures before durable recording are absent.",
                "The window is today and the six previous UTC calendar days; today is partial.",
                "Known cost sums priced records only. Unpriced calls are counted separately, "
                "not estimated as free.",
                "Recent requests are the latest 20 recorded completions in this window "
                "and contain no prompt or answer text.",
                "Memory counts cover all retained source records in this scope; "
                "private requests and cache-hit copies are excluded.",
                "Runtime values are configured models, devices and limits; "
                "they do not measure available hardware or throughput.",
                "Health checks measure service reachability and embedding worker state; "
                "no generation or embedding inference is probed.",
            ],
        }
    )
    return result


@router.get("/graph")
async def memory_graph(
    request: Request,
    principal: Principal = Depends(authenticate),
    user_id: str = Query(min_length=1, max_length=128, pattern=SCOPE_PATTERN),
    feature_tag: str = Query(min_length=1, max_length=128, pattern=SCOPE_PATTERN),
    limit: int = Query(default=60, ge=1, le=80),
):
    await admit(request, principal, user_id)
    state = request.app.state
    boundary = scope(principal, user_id, feature_tag)
    initial = await state.db.console_graph_snapshot(**boundary, limit=limit)
    ids = [row["id"] for row in initial["nodes"]]
    projection = {"edges": [], "truncated": False}
    degraded = False
    try:
        if ids:
            async with asyncio.timeout(settings.graph_timeout_seconds):
                projection = await state.graph.console_edges(ids, **boundary, limit=320)
        else:
            degraded = not await healthy(state.graph)
    except Exception:
        # PostgreSQL remains useful when the asynchronous projection is unavailable.
        # Never return exception strings, which may contain query data or credentials.
        degraded = True
    result = await state.db.console_graph_snapshot(
        **boundary,
        limit=limit,
        ids=ids,
        expected_revision=initial["scope_revision"],
    )
    active = {row["id"] for row in result["nodes"]}
    result.update(
        {
            "edges": [
                edge for edge in projection["edges"] if edge["source"] in active and edge["target"] in active
            ],
            "limits": {"nodes": limit, "edges": 320},
            "truncated": {
                "nodes": result["total_active_nodes"] > len(result["nodes"]),
                "edges": projection["truncated"],
            },
            "degraded": degraded,
            "limitations": [
                "Nodes are recent active PostgreSQL memories, "
                "limited to this authenticated user and feature scope.",
                "Only actual SIMILAR_TO and INFORMED_BY relationships among displayed nodes "
                "are included; no edges are inferred.",
                "Neo4j is an asynchronous projection: edges can lag, "
                "and an empty graph does not prove memories are unrelated.",
                "Prompt previews are reloaded from PostgreSQL after the graph query; "
                "a concurrent memory mutation requires refresh.",
            ]
            + (
                [
                    "The graph service is unavailable; "
                    "authoritative memory nodes are shown without relationships."
                ]
                if degraded
                else []
            ),
        }
    )
    return result

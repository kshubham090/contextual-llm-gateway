"""Authenticated memory lifecycle. PostgreSQL is the live-content authority."""

import asyncio
import hashlib
import json
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .auth import Principal, authenticate
from .config import settings
from .db import MemoryConflict, MemoryNotFound
from .embeddings import EmbeddingError, EmbeddingOverloadedError


class MemoryRoute(APIRoute):
    def get_route_handler(self):
        original = super().get_route_handler()

        async def handler(request: Request):
            try:
                async with asyncio.timeout(settings.request_timeout_seconds):
                    return await original(request)
            except MemoryNotFound:
                raise HTTPException(404, "Memory not found in this scope")
            except MemoryConflict:
                raise HTTPException(409, "Memory changed; refresh its revision and retry")
            except EmbeddingOverloadedError:
                raise HTTPException(503, "Embedding capacity unavailable", headers={"Retry-After": "1"})
            except EmbeddingError:
                raise HTTPException(502, "Embedding service unavailable")
            except TimeoutError:
                raise HTTPException(504, "Memory operation deadline exceeded")
            except ValueError:
                raise HTTPException(422, "Invalid memory request or cursor")

        return handler


router = APIRouter(prefix="/v1/memories", tags=["memory"], route_class=MemoryRoute)
SCOPE_PATTERN = r"^[\w.@:-]+$"


class MemoryContent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str = Field(min_length=1, max_length=128, pattern=SCOPE_PATTERN)
    feature_tag: str = Field(min_length=1, max_length=128, pattern=SCOPE_PATTERN)
    prompt: str = Field(min_length=1, max_length=64000)
    response: str = Field(min_length=1, max_length=64000)

    @field_validator("prompt", "response")
    @classmethod
    def text_is_valid(cls, value):
        if not value.strip() or "\x00" in value:
            raise ValueError("Memory must contain text and no null bytes")
        return value


class CreateMemory(MemoryContent):
    expected_scope_revision: int = Field(ge=0)


class CorrectMemory(MemoryContent):
    expected_revision: int = Field(ge=1)


async def admit(request, principal, user_id):
    if not request.app.state.ready:
        raise HTTPException(503, "Gateway is not ready", headers={"Retry-After": "1"})
    allowed, retry = await request.app.state.limiter.check(user_id, tenant_id=principal.tenant_id)
    if not allowed:
        raise HTTPException(429, "Rate limit exceeded", headers={"Retry-After": str(retry)})


def scope(principal, user_id, feature_tag):
    return {"tenant_id": principal.tenant_id, "user_id": user_id, "feature_tag": feature_tag}


@router.get("")
async def list_memories(
    request: Request, principal: Principal = Depends(authenticate),
    user_id: str = Query(min_length=1, max_length=128, pattern=SCOPE_PATTERN),
    feature_tag: str = Query(min_length=1, max_length=128, pattern=SCOPE_PATTERN),
    limit: int = Query(default=50, ge=1, le=100),
    cursor: str | None = Query(default=None, max_length=2048),
):
    await admit(request, principal, user_id)
    return await request.app.state.db.list_memories(
        **scope(principal, user_id, feature_tag), limit=limit, cursor=cursor,
    )


@router.get("/{memory_id}")
async def memory_detail(
    memory_id: uuid.UUID, request: Request, principal: Principal = Depends(authenticate),
    user_id: str = Query(min_length=1, max_length=128, pattern=SCOPE_PATTERN),
    feature_tag: str = Query(min_length=1, max_length=128, pattern=SCOPE_PATTERN),
):
    await admit(request, principal, user_id)
    return await request.app.state.db.memory_detail(memory_id, **scope(principal, user_id, feature_tag))


async def write_memory(request, principal, body, memory_id=None):
    await admit(request, principal, body.user_id)
    boundary = scope(principal, body.user_id, body.feature_tag)
    # The creation/correction revision is checked before and after embedding.
    # Rejected scope probes must not send someone else's content to a provider.
    if memory_id is not None:
        old = await request.app.state.db.memory_detail(memory_id, **boundary)
        if old["revision"] != body.expected_revision or old["status"] != "active":
            raise MemoryConflict()
    elif await request.app.state.db.memory_epoch(**boundary) != body.expected_scope_revision:
        raise MemoryConflict()
    namespace = hashlib.sha256(json.dumps(list(boundary.values())).encode()).hexdigest()
    embedding = await request.app.state.embedder.embed(body.prompt, namespace=namespace, cache=False)
    options = ({"expected_scope_revision": body.expected_scope_revision} if memory_id is None else {
        "supersedes_id": memory_id, "expected_revision": body.expected_revision,
    })
    return await request.app.state.db.create_memory(
        **boundary, prompt=body.prompt, response=body.response, embedding=embedding,
        embedding_space=request.app.state.embedder.space_id, **options,
    )


@router.post("", status_code=201)
async def create_memory(body: CreateMemory, request: Request, principal: Principal = Depends(authenticate)):
    return await write_memory(request, principal, body)


@router.patch("/{memory_id}")
@router.post("/{memory_id}/corrections", status_code=201)
async def correct_memory(
    memory_id: uuid.UUID, body: CorrectMemory, request: Request,
    principal: Principal = Depends(authenticate),
):
    return await write_memory(request, principal, body, memory_id)


@router.delete("")
async def delete_scope(
    request: Request, principal: Principal = Depends(authenticate),
    user_id: str = Query(min_length=1, max_length=128, pattern=SCOPE_PATTERN),
    feature_tag: str = Query(min_length=1, max_length=128, pattern=SCOPE_PATTERN),
    expected_scope_revision: int = Query(ge=0),
):
    await admit(request, principal, user_id)
    return await request.app.state.db.delete_memory(
        None, **scope(principal, user_id, feature_tag), expected_revision=expected_scope_revision,
    )


@router.delete("/{memory_id}")
async def delete_memory(
    memory_id: uuid.UUID, request: Request, principal: Principal = Depends(authenticate),
    user_id: str = Query(min_length=1, max_length=128, pattern=SCOPE_PATTERN),
    feature_tag: str = Query(min_length=1, max_length=128, pattern=SCOPE_PATTERN),
    expected_revision: int = Query(ge=1),
):
    await admit(request, principal, user_id)
    return await request.app.state.db.delete_memory(
        memory_id, **scope(principal, user_id, feature_tag), expected_revision=expected_revision,
    )

from contextlib import asynccontextmanager

import anthropic
import httpx
from fastapi import FastAPI, HTTPException

from .db import Database
from .embeddings import EmbeddingClient
from .graph import MemoryGraph
from .pipeline import Pipeline, RateLimitExceeded
from .providers import Router
from .rate_limit import RateLimiter
from .schemas import ChatRequest, ChatResponse, UsageRow

db = Database()
graph = MemoryGraph()
embedder = EmbeddingClient()
rate_limiter = RateLimiter()
router = Router()
pipeline = Pipeline(db, graph, embedder, rate_limiter, router)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.connect()
    await graph.connect()
    await rate_limiter.connect()
    yield
    await db.close()
    await graph.close()
    await rate_limiter.close()
    await embedder.close()


app = FastAPI(
    title="Contextual LLM Gateway",
    description="An LLM gateway with a Neo4j-backed memory graph: every call is "
    "embedded, linked to related past calls, and used to ground future requests.",
    lifespan=lifespan,
)


@app.post("/v1/chat", response_model=ChatResponse)
async def chat(req: ChatRequest) -> ChatResponse:
    try:
        return await pipeline.handle_chat(req)
    except RateLimitExceeded as e:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={"Retry-After": str(e.retry_after)},
        )
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=502, detail=f"Embedding service error: {e}")
    except anthropic.APIStatusError as e:
        # Both primary and fallback model failed
        raise HTTPException(status_code=502, detail=f"LLM provider error ({e.status_code})")
    except anthropic.APIConnectionError:
        raise HTTPException(status_code=502, detail="LLM provider unreachable")


@app.get("/v1/usage", response_model=list[UsageRow])
async def usage(user_id: str | None = None, feature_tag: str | None = None):
    """Cost/usage rollup by user, feature, and day."""
    return await db.usage_rollup(user_id=user_id, feature_tag=feature_tag)


@app.get("/v1/graph/stats")
async def graph_stats():
    """Node/edge counts — watch the memory graph grow."""
    return await graph.stats()


@app.get("/health")
async def health():
    return {"status": "ok"}

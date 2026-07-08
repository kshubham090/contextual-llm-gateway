"""The request pipeline — the full flow from the spec:

    rate limit → embed → exact-cache check → graph context retrieval
    → augmented LLM call (routed, with fallback) → write-back → respond
"""
import asyncio
import uuid
from time import perf_counter

from .config import settings
from .db import Database
from .embeddings import EmbeddingClient
from .graph import MemoryGraph
from .providers import Router, estimate_cost
from .rate_limit import RateLimiter
from .schemas import ChatMetadata, ChatRequest, ChatResponse


class RateLimitExceeded(Exception):
    def __init__(self, retry_after: int) -> None:
        self.retry_after = retry_after


def _trim(text: str) -> str:
    limit = settings.context_snippet_chars
    return text if len(text) <= limit else text[:limit] + " …"


def build_context_blob(context_calls: list[dict]) -> str:
    """Compact system-prompt block of related past exchanges."""
    parts = [
        "You are answering through a gateway that remembers related past "
        "interactions in this domain. The exchanges below are prior calls "
        "semantically related to the current request. Use them as background "
        "knowledge where relevant; ignore them where they don't apply. Do not "
        "mention this context mechanism to the user.",
        "",
        "<related_past_calls>",
    ]
    for i, call in enumerate(context_calls, 1):
        parts.append(f"[{i}] (feature: {call.get('feature_tag', 'unknown')})")
        parts.append(f"Q: {_trim(call['prompt'])}")
        if call.get("response"):
            parts.append(f"A: {_trim(call['response'])}")
        parts.append("")
    parts.append("</related_past_calls>")
    return "\n".join(parts)


class Pipeline:
    def __init__(
        self,
        db: Database,
        graph: MemoryGraph,
        embedder: EmbeddingClient,
        rate_limiter: RateLimiter,
        router: Router,
    ) -> None:
        self.db = db
        self.graph = graph
        self.embedder = embedder
        self.rate_limiter = rate_limiter
        self.router = router

    async def handle_chat(self, req: ChatRequest) -> ChatResponse:
        allowed, retry_after = await self.rate_limiter.check(req.user_id)
        if not allowed:
            raise RateLimitExceeded(retry_after)

        start = perf_counter()
        call_id = uuid.uuid4()

        # 1. Embed the prompt once — reused for cache check, graph seeding,
        #    and write-back.
        embedding = await self.embedder.embed(req.prompt)

        # 2. One pgvector query serves both tiers: >= 0.95 is a cache hit,
        #    >= 0.75 is "related" and seeds the graph walk.
        similar = await self.db.find_similar(
            embedding,
            limit=max(10, settings.graph_context_limit),
            min_similarity=settings.graph_similarity_threshold,
        )

        # 3. Fast path: near-exact repeat → serve the cached response.
        if req.use_cache and similar and similar[0]["similarity"] >= settings.cache_hit_threshold:
            cached = similar[0]
            latency_ms = int((perf_counter() - start) * 1000)
            await asyncio.gather(
                self.db.log_call(
                    call_id=call_id,
                    user_id=req.user_id,
                    feature_tag=req.feature_tag,
                    prompt=req.prompt,
                    response=None,  # response lives on the original row
                    model=None,
                    provider=None,
                    tokens_in=0,
                    tokens_out=0,
                    cost=0.0,
                    latency_ms=latency_ms,
                    cache_hit=True,
                    fallback_used=False,
                    embedding=embedding,
                ),
                self.graph.write_cache_hit(
                    call_id=str(call_id),
                    user_id=req.user_id,
                    feature_tag=req.feature_tag,
                    prompt=req.prompt,
                    cached_call_id=cached["id"],
                    similarity=cached["similarity"],
                ),
            )
            return ChatResponse(
                response=cached["response"],
                meta=ChatMetadata(
                    call_id=str(call_id),
                    cache_hit=True,
                    cached_call_id=cached["id"],
                    latency_ms=latency_ms,
                ),
            )

        # 4. Graph context retrieval — the differentiator. Vector-similar
        #    calls seed a 1–2 hop Neo4j walk that pulls in the topical
        #    cluster (follow-ups, sibling calls) vector search alone misses.
        context_calls: list[dict] = []
        if req.use_graph and similar:
            seed_ids = [s["id"] for s in similar]
            context_calls = await self.graph.expand_neighborhood(
                seed_ids, settings.graph_context_limit
            )
            if not context_calls:
                # Graph lagging behind Postgres (e.g. fresh restore) — the
                # vector matches themselves are still useful context.
                context_calls = similar[: settings.graph_context_limit]

        system = build_context_blob(context_calls) if context_calls else None

        # 5. Routed LLM call with automatic fallback on 429/timeout/5xx.
        result = await self.router.complete(
            req.prompt, system, req.max_tokens or settings.default_max_tokens
        )
        cost = estimate_cost(result.model, result.tokens_in, result.tokens_out)
        latency_ms = int((perf_counter() - start) * 1000)
        informed_by = [c["id"] for c in context_calls]

        # 6. Write-back: Postgres cost row + graph node, in parallel.
        #    store=False (demo/benchmark calls) still logs the cost row, but
        #    without an embedding and without a graph node, so the call can
        #    never be served from cache or injected as context later.
        log_task = self.db.log_call(
            call_id=call_id,
            user_id=req.user_id,
            feature_tag=req.feature_tag,
            prompt=req.prompt,
            response=result.text,
            model=result.model,
            provider=result.provider,
            tokens_in=result.tokens_in,
            tokens_out=result.tokens_out,
            cost=cost,
            latency_ms=latency_ms,
            cache_hit=False,
            fallback_used=result.fallback_used,
            embedding=embedding if req.store else None,
        )
        if req.store:
            await asyncio.gather(
                log_task,
                self.graph.write_call(
                    call_id=str(call_id),
                    user_id=req.user_id,
                    feature_tag=req.feature_tag,
                    prompt=req.prompt,
                    response=result.text,
                    model=result.model,
                    provider=result.provider,
                    fallback_provider=result.fallback_provider if result.fallback_used else None,
                    tokens_in=result.tokens_in,
                    tokens_out=result.tokens_out,
                    cost=cost,
                    latency_ms=latency_ms,
                    similar=[{"id": s["id"], "score": s["similarity"]} for s in similar],
                    informed_by=informed_by,
                ),
            )
        else:
            await log_task

        return ChatResponse(
            response=result.text,
            meta=ChatMetadata(
                call_id=str(call_id),
                cache_hit=False,
                context_used=informed_by,
                model=result.model,
                provider=result.provider,
                fallback_used=result.fallback_used,
                tokens_in=result.tokens_in,
                tokens_out=result.tokens_out,
                cost=round(cost, 6),
                latency_ms=latency_ms,
            ),
        )

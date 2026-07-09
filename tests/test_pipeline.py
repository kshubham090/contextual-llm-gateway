"""Pipeline behavior with every external dependency faked — no Postgres,
Neo4j, Redis, Voyage, or Anthropic needed."""
import time

import pytest

from app.pipeline import Pipeline, RateLimitExceeded
from app.providers import CompletionResult
from app.schemas import ChatRequest


class FakeDB:
    def __init__(self, similar=None):
        self.similar = similar or []
        self.logged: list[dict] = []

    async def find_similar(self, embedding, limit, min_similarity):
        return self.similar

    async def log_call(self, **kw):
        self.logged.append(kw)


class FakeGraph:
    def __init__(self, pool=None):
        self.pool = pool or []
        self.calls_written: list[dict] = []
        self.cache_hits_written: list[dict] = []

    async def expand_neighborhood(self, seed_ids, limit):
        return self.pool

    async def write_call(self, **kw):
        self.calls_written.append(kw)

    async def write_cache_hit(self, **kw):
        self.cache_hits_written.append(kw)


class FakeEmbedder:
    async def embed(self, text):
        return [0.1, 0.2, 0.3]


class FakeLimiter:
    def __init__(self, allowed=True):
        self.allowed = allowed

    async def check(self, user_id):
        return (True, 0) if self.allowed else (False, 42)


class FakeRouter:
    def __init__(self):
        self.seen_system = None
        self.seen_prompt = None

    async def complete(self, prompt, system, max_tokens):
        self.seen_prompt = prompt
        self.seen_system = system
        return CompletionResult(
            text="llm answer", model="claude-haiku-4-5", provider="anthropic",
            tokens_in=100, tokens_out=50,
            fallback_used=False, fallback_provider=None,
        )


def _similar_call(id_="past-1", similarity=0.85):
    return {
        "id": id_,
        "prompt": "How do Orbit rollbacks work?",
        "response": "Image reverts automatically; config needs a separate revert.",
        "feature_tag": "deploys",
        "created_epoch": time.time(),
        "similarity": similarity,
    }


def _pipeline(db=None, graph=None, limiter=None, router=None):
    return Pipeline(
        db=db or FakeDB(),
        graph=graph or FakeGraph(),
        embedder=FakeEmbedder(),
        rate_limiter=limiter or FakeLimiter(),
        router=router or FakeRouter(),
    )


def _req(**overrides):
    base = {"prompt": "What happens on a bad deploy?", "user_id": "u1", "feature_tag": "deploys"}
    return ChatRequest(**{**base, **overrides})


async def test_rate_limit_raises_with_retry_after():
    p = _pipeline(limiter=FakeLimiter(allowed=False))
    with pytest.raises(RateLimitExceeded) as exc:
        await p.handle_chat(_req())
    assert exc.value.retry_after == 42


async def test_cache_hit_serves_cached_response_and_writes_back():
    db = FakeDB(similar=[_similar_call(similarity=0.97)])
    graph = FakeGraph()
    router = FakeRouter()
    p = _pipeline(db=db, graph=graph, router=router)

    resp = await p.handle_chat(_req())
    await p.drain()  # flush background write-backs

    assert resp.meta.cache_hit is True
    assert resp.response == "Image reverts automatically; config needs a separate revert."
    assert resp.meta.cached_call_id == "past-1"
    assert router.seen_prompt is None  # LLM never called
    assert len(db.logged) == 1 and db.logged[0]["cache_hit"] is True
    assert db.logged[0]["cost"] == 0.0
    assert len(graph.cache_hits_written) == 1


async def test_use_cache_false_bypasses_cache():
    db = FakeDB(similar=[_similar_call(similarity=0.99)])
    router = FakeRouter()
    p = _pipeline(db=db, router=router)

    resp = await p.handle_chat(_req(use_cache=False))
    await p.drain()

    assert resp.meta.cache_hit is False
    assert resp.response == "llm answer"  # LLM ran despite the 0.99 match


async def test_graph_context_is_injected_into_system_prompt():
    db = FakeDB(similar=[_similar_call(similarity=0.85)])
    graph = FakeGraph(pool=[
        {"id": "past-1", "prompt": "q1", "response": "a1", "feature_tag": "deploys",
         "created_epoch": time.time()},
        {"id": "past-2", "prompt": "q2", "response": "a2", "feature_tag": "deploys",
         "created_epoch": time.time()},
    ])
    router = FakeRouter()
    p = _pipeline(db=db, graph=graph, router=router)

    resp = await p.handle_chat(_req())
    await p.drain()

    assert "<related_past_calls>" in router.seen_system
    assert set(resp.meta.context_used) == {"past-1", "past-2"}
    # write-back records the proof-of-value edge targets
    assert set(graph.calls_written[0]["informed_by"]) == {"past-1", "past-2"}


async def test_empty_graph_falls_back_to_vector_matches():
    db = FakeDB(similar=[_similar_call(similarity=0.85)])
    graph = FakeGraph(pool=[])  # Neo4j has nothing yet
    router = FakeRouter()
    p = _pipeline(db=db, graph=graph, router=router)

    resp = await p.handle_chat(_req())
    await p.drain()

    assert resp.meta.context_used == ["past-1"]
    assert "Orbit rollbacks" in router.seen_system


async def test_use_graph_false_skips_context():
    db = FakeDB(similar=[_similar_call(similarity=0.85)])
    router = FakeRouter()
    p = _pipeline(db=db, router=router)

    resp = await p.handle_chat(_req(use_graph=False))
    await p.drain()

    assert router.seen_system is None
    assert resp.meta.context_used == []


async def test_store_false_logs_cost_but_stays_out_of_memory():
    db = FakeDB()
    graph = FakeGraph()
    p = _pipeline(db=db, graph=graph)

    await p.handle_chat(_req(store=False))
    await p.drain()

    assert len(db.logged) == 1
    assert db.logged[0]["embedding"] is None      # never a future cache/context hit
    assert graph.calls_written == []              # no graph node


async def test_write_back_happens_off_the_request_path():
    db = FakeDB()
    graph = FakeGraph()
    p = _pipeline(db=db, graph=graph)

    resp = await p.handle_chat(_req())
    # response returned; persistence may still be in flight
    assert resp.response == "llm answer"
    await p.drain()
    assert len(db.logged) == 1
    assert len(graph.calls_written) == 1
    assert p._bg_tasks == set()  # drained clean


async def test_cost_and_metadata_are_reported():
    p = _pipeline()
    resp = await p.handle_chat(_req())
    await p.drain()

    # 100 in + 50 out on haiku: 100*1/1M + 50*5/1M
    assert resp.meta.cost == pytest.approx(0.00035)
    assert resp.meta.model == "claude-haiku-4-5"
    assert resp.meta.tokens_in == 100
    assert resp.meta.latency_ms >= 0

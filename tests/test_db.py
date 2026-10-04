"""Persistence boundaries and atomicity without external services."""

import copy
import json
import uuid
from datetime import UTC, datetime

import pytest

from app.db import Database, _vec


class CapturePool:
    def __init__(self, rows=()):
        self.rows = rows
        self.queries = []
        self.calls = {}
        self.outbox = {}
        self.fail_outbox = False

    def acquire(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def transaction(self):
        pool = self

        class Transaction:
            async def __aenter__(self):
                self.before = copy.deepcopy((pool.calls, pool.outbox))

            async def __aexit__(self, exc_type, *args):
                if exc_type:
                    pool.calls, pool.outbox = self.before

        return Transaction()

    async def fetch(self, sql, *args):
        self.queries.append((sql, args))
        return self.rows

    async def fetchval(self, sql, *args):
        self.queries.append((sql, args))
        assert "INSERT INTO calls" in sql
        if args[0] in self.calls:
            return None
        self.calls[args[0]] = args
        return args[0]

    async def fetchrow(self, sql, *args):
        self.queries.append((sql, args))
        if "md5(prompt)" in sql:
            return self.rows[0] if self.rows else None
        row = self.calls.get(args[0])
        return dict(zip(("tenant_id", "user_id", "feature_tag"), row[1:4])) if row else None

    async def execute(self, sql, *args):
        self.queries.append((sql, args))
        if "INSERT INTO graph_outbox" in sql:
            if self.fail_outbox:
                raise RuntimeError("simulated outbox failure")
            self.outbox[args[0]] = json.loads(args[2])


def call_arguments(**overrides):
    return {
        "call_id": uuid.uuid4(), "tenant_id": "tenant-a", "user_id": "user-a",
        "feature_tag": "research", "prompt": "private prompt", "response": "private answer",
        "model": "test-model", "provider": "test", "tokens_in": 3, "tokens_out": 4,
        "cost": 0.1, "latency_ms": 25, "cache_hit": False, "fallback_used": False,
        "embedding": [1.0, 0.0, 0.0], "max_tokens": 128, "embedding_space": "test:3",
        "generation_config": "route-v1", "graph_event": {"kind": "call", "payload": {}},
        **overrides,
    }


async def test_retrieval_binds_every_scope_dimension_and_separates_cache_config():
    pool = CapturePool(rows=[{
        "id": uuid.uuid4(), "prompt": "p", "response": "r", "feature_tag": "research",
        "created_epoch": 1000, "similarity": 0.99, "cache_eligible": False,
    }])
    db = Database()
    db.pool = pool
    result = await db.find_similar(
        [1, 0, 0], 10000, 0.7, tenant_id="tenant-a", user_id="user-a", feature_tag="research",
        max_tokens=256, embedding_space="test:3", generation_config="route-v2",
    )
    sql, args = pool.queries[0]
    assert "tenant_id = $3 AND user_id = $4 AND feature_tag = $5" in sql
    assert "embedding_space = $7" in sql and "expires_at > now()" in sql
    assert "max_tokens = $6 AND generation_config = $8" in sql
    assert "cache_expires_at > now()" in sql
    assert "ORDER BY embedding <=> $1::vector" in sql
    assert args[1:] == (200, "tenant-a", "user-a", "research", 256, "test:3", "route-v2")
    assert result[0]["cache_eligible"] is False  # still usable as context


async def test_cost_and_outbox_are_atomic_and_retries_do_not_duplicate():
    db = Database()
    db.pool = pool = CapturePool()
    arguments = call_arguments()
    pool.fail_outbox = True
    with pytest.raises(RuntimeError):
        await db.log_call(**arguments)
    assert pool.calls == {} and pool.outbox == {}
    pool.fail_outbox = False
    await db.log_call(**arguments)
    await db.log_call(**arguments)
    assert len(pool.calls) == len(pool.outbox) == 1
    payload = pool.outbox[arguments["call_id"]]["payload"]
    assert payload["tenant_id"] == "tenant-a"
    assert datetime.fromisoformat(payload["expires_at"]) > datetime.now(UTC)


async def test_no_store_defensively_strips_response_embedding_and_graph_payload():
    db = Database()
    db.pool = pool = CapturePool()
    await db.log_call(**call_arguments(prompt=None))
    row = next(iter(pool.calls.values()))
    assert row[4] is None and row[5] is None and row[14] is None
    assert row[10] == 0.1  # accounting remains
    assert pool.outbox == {}


async def test_graph_payload_cannot_override_persisted_scope():
    db = Database()
    db.pool = pool = CapturePool()
    args = call_arguments(graph_event={"kind": "call", "payload": {
        "tenant_id": "other", "user_id": "other", "feature_tag": "other", "prompt": "other",
    }})
    await db.log_call(**args)
    payload = pool.outbox[args["call_id"]]["payload"]
    assert (payload["tenant_id"], payload["user_id"], payload["feature_tag"]) == (
        "tenant-a", "user-a", "research"
    )
    assert payload["prompt"] == args["prompt"]


async def test_call_id_cannot_be_reused_across_tenants():
    db = Database()
    db.pool = CapturePool()
    args = call_arguments()
    await db.log_call(**args)
    with pytest.raises(ValueError, match="different scope"):
        await db.log_call(**{**args, "tenant_id": "tenant-b"})


async def test_legacy_quarantine_is_never_a_valid_tenant():
    db = Database()
    with pytest.raises(ValueError, match="tenant"):
        await db.log_call(**call_arguments(tenant_id="__legacy_quarantine__"))


@pytest.mark.parametrize("embedding", [[], [0.0, 0.0], [float("nan")], [float("inf")]])
def test_vector_rejects_nonfinite_input(embedding):
    with pytest.raises(ValueError):
        _vec(embedding)


async def test_usage_query_is_tenant_scoped_bounded_and_marks_unknown_prices():
    db = Database()
    db.pool = pool = CapturePool()
    await db.usage_rollup("user-a", "research", tenant_id="tenant-a", limit=9000)
    sql, args = pool.queries[0]
    assert "tenant_id = $1" in sql and "LIMIT $4" in sql
    assert "cost IS NULL" in sql and "unpriced_calls" in sql
    assert args == ("tenant-a", "user-a", "research", 1000)


async def test_claims_serialize_each_scope_and_use_skip_locked_and_lease_fencing():
    db = Database()
    db.pool = pool = CapturePool()
    await db.claim_graph_events(limit=500)
    sql, args = pool.queries[0]
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "earlier.tenant_id = candidate.tenant_id" in sql
    assert "earlier.user_id = candidate.user_id" in sql
    assert "earlier.feature_tag = candidate.feature_tag" in sql
    assert "earlier.id < candidate.id" in sql
    assert args[0] == 200 and isinstance(args[2], uuid.UUID)
    await db.ack_graph_event(1, args[2])
    assert "lease_token = $2" in pool.queries[-1][0]


async def test_exact_cache_lookup_binds_scope_policy_and_full_prompt_independently_of_vectors():
    row = {
        "id": uuid.uuid4(), "prompt": "exact prompt", "response": "cached response",
        "feature_tag": "research", "created_epoch": 1000,
    }
    db = Database()
    db.pool = pool = CapturePool(rows=[row])
    result = await db.find_exact(
        "exact prompt", tenant_id="tenant-a", user_id="user-a", feature_tag="research",
        max_tokens=256, embedding_space="test:3", generation_config="policy-v2",
    )
    sql, args = pool.queries[0]
    assert "md5(prompt) = md5($1) AND prompt = $1" in sql
    assert "tenant_id = $2 AND user_id = $3 AND feature_tag = $4" in sql
    assert "max_tokens = $5 AND embedding_space = $6 AND generation_config = $7" in sql
    assert "expires_at > now() AND cache_expires_at > now()" in sql
    assert "response IS NOT NULL AND NOT cache_hit" in sql and "embedding IS NOT NULL" in sql
    assert "ORDER BY created_at DESC, id DESC" in sql and "LIMIT 1" in sql
    assert "<=>" not in sql
    assert args == ("exact prompt", "tenant-a", "user-a", "research", 256, "test:3", "policy-v2")
    assert result == {**row, "id": str(row["id"]), "created_epoch": 1000.0,
                      "similarity": 1.0, "cache_eligible": True}


async def test_exact_cache_miss_returns_none():
    db = Database()
    db.pool = CapturePool()
    assert await db.find_exact(
        "missing", tenant_id="tenant-a", user_id="user-a", feature_tag="research",
        max_tokens=256, embedding_space="test:3", generation_config="policy-v2",
    ) is None

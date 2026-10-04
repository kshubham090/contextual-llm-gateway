"""Scope/path regression guards; live graph semantics live in test_services.py."""

from datetime import UTC, datetime, timedelta

import pytest

from app.graph import MemoryGraph, _key


class Result:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    async def consume(self):
        pass

    async def single(self):
        return {"calls": 0, "similar_edges": 0, "informed_by_edges": 0}


class Driver:
    def __init__(self):
        self.queries = []
        self.transactions = 0

    def session(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def execute_write(self, function):
        self.transactions += 1
        return await function(self)

    async def run(self, query, **parameters):
        self.queries.append((str(query), parameters))
        return Result()


def graph_arguments(**overrides):
    return dict(
        call_id="call-a", tenant_id="tenant-a", user_id="user-a", feature_tag="research",
        prompt="private", response="answer", model="model", provider="provider",
        fallback_provider="fallback", tokens_in=1, tokens_out=2, cost=0.1, latency_ms=10,
        similar=[{"id": "past-a", "score": 0.9}], informed_by=["past-a"], **overrides,
    )


async def test_expansion_filters_every_node_and_relationship_in_path():
    graph = MemoryGraph()
    graph.driver = driver = Driver()
    await graph.expand_neighborhood(
        ["a"], 20, tenant_id="tenant-a", user_id="user-a", feature_tag="research"
    )
    query, params = driver.queries[0]
    assert "all(node IN nodes(path)" in query
    assert "all(edge IN relationships(path)" in query
    for part in ("node", "edge"):
        for key in ("tenant_id", "user_id", "feature_tag"):
            assert f"{part}.{key} = ${key}" in query
        assert f"{part}.expires_at > datetime()" in query
    assert "node:GatewayCall" in query
    assert params["tenant_id"] == "tenant-a"


async def test_graph_write_is_one_transaction_with_idempotent_nodes_and_scoped_targets():
    graph = MemoryGraph()
    graph.driver = driver = Driver()
    await graph.write_call(**graph_arguments())
    assert driver.transactions == 1
    query = driver.queries[0][0]
    assert "MERGE (c:GatewayCall" in query and "ON CREATE SET" in query
    for query, params in driver.queries[2:]:
        for key in ("tenant_id", "user_id", "feature_tag"):
            assert f"{key}: ${key}" in query
        assert "o.expires_at > datetime()" in query
        assert params["tenant_id"] == "tenant-a"


async def test_expired_outbox_replay_never_recreates_expired_graph_memory():
    graph = MemoryGraph()
    graph.driver = driver = Driver()
    yesterday = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    await graph.write_call(**graph_arguments(expires_at=yesterday))
    assert driver.queries == []


def test_scope_keys_cannot_collide_on_separator_characters_or_tenant_boundaries():
    assert _key("a:b", "c") != _key("a", "b:c")
    assert _key("tenant-a", "u", "f", "id") != _key("tenant-b", "u", "f", "id")


async def test_graph_stats_avoid_cartesian_product_and_scope_both_edge_ends():
    graph = MemoryGraph()
    graph.driver = driver = Driver()
    assert await graph.stats(tenant_id="tenant-a") == {
        "calls": 0, "similar_edges": 0, "informed_by_edges": 0,
    }
    query, params = driver.queries[0]
    assert query.count("CALL {") == 3
    assert "o.tenant_id = c.tenant_id" in query and "r.tenant_id = c.tenant_id" in query
    assert params["tenant_id"] == "tenant-a"


async def test_graph_rejects_quarantine_scope():
    graph = MemoryGraph()
    with pytest.raises(ValueError):
        await graph.stats(tenant_id="__legacy_quarantine__")

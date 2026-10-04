"""Opt-in integration tests against disposable PostgreSQL/pgvector and Neo4j.

Run RUN_SERVICE_TESTS=1 pytest -m integration after configuring DATABASE_URL and
NEO4J_* for a test stack. Data is uniquely scoped and removed by each fixture.
"""

import asyncio
import os
import uuid
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio

from app.config import settings
from app.db import Database
from app.graph import MemoryGraph
from app.outbox import GraphOutboxWorker

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_SERVICE_TESTS") != "1", reason="RUN_SERVICE_TESTS=1 required"),
]


def vector():
    return [1.0] + [0.0] * (settings.embedding_dim - 1)


def call_arguments(tenant, **overrides):
    base = {
        "call_id": uuid.uuid4(), "tenant_id": tenant, "user_id": "reader", "feature_tag": "research",
        "prompt": "private fact", "response": "private answer", "model": "test-model",
        "provider": "test-provider", "tokens_in": 2, "tokens_out": 3, "cost": 0.01,
        "latency_ms": 5, "cache_hit": False, "fallback_used": False, "embedding": vector(),
        "max_tokens": 128, "embedding_space": "integration-v1", "generation_config": "policy-v1",
        "graph_event": None,
    }
    return {**base, **overrides}


def graph_arguments(tenant, **overrides):
    base = {
        "call_id": str(uuid.uuid4()), "tenant_id": tenant, "user_id": "reader", "feature_tag": "research",
        "prompt": "private fact", "response": "private answer", "model": "test-model",
        "provider": "test-provider", "tokens_in": 2, "tokens_out": 3, "cost": 0.01,
        "latency_ms": 5, "fallback_provider": None, "similar": [], "informed_by": [],
    }
    return {**base, **overrides}


@pytest_asyncio.fixture
async def services():
    tenant = f"integration-{uuid.uuid4()}"
    other = tenant + "-other"
    db, graph = Database(), MemoryGraph()
    await db.connect()
    try:
        await graph.connect()
        yield db, graph, tenant, other
    finally:
        await db.pool.execute("DELETE FROM calls WHERE tenant_id = ANY($1::text[])", [tenant, other])
        if graph.driver:
            async with graph.driver.session() as session:
                result = await session.run(
                    "MATCH (n) WHERE n.tenant_id IN $tenants DETACH DELETE n", tenants=[tenant, other]
                )
                await result.consume()
        await graph.close()
        await db.close()


async def test_services_vector_scope_config_ttl_and_private_accounting(services):
    db, graph, tenant, other = services
    own = call_arguments(tenant)
    alternatives = [
        call_arguments(other), call_arguments(tenant, user_id="other-user"),
        call_arguments(tenant, feature_tag="other-feature"),
        call_arguments(tenant, embedding_space="integration-v2"),
    ]
    expired = call_arguments(tenant)
    wrong_budget = call_arguments(tenant, max_tokens=256)
    wrong_policy = call_arguments(tenant, generation_config="policy-v2")
    cache_expired = call_arguments(tenant)
    private = call_arguments(tenant, prompt=None)
    for args in [own, *alternatives, expired, wrong_budget, wrong_policy, cache_expired, private]:
        await db.log_call(**args)
    await db.pool.execute("UPDATE calls SET expires_at = now() - interval '1 second' WHERE id = $1",
                          expired["call_id"])
    await db.pool.execute("UPDATE calls SET cache_expires_at = now() - interval '1 second' WHERE id = $1",
                          cache_expired["call_id"])
    hits = await db.find_similar(
        vector(), 100, 0.9, tenant_id=tenant, user_id="reader", feature_tag="research",
        max_tokens=128, embedding_space="integration-v1", generation_config="policy-v1",
    )
    assert {row["id"] for row in hits} == {
        str(item["call_id"]) for item in [own, wrong_budget, wrong_policy, cache_expired]
    }
    assert {row["id"] for row in hits if row["cache_eligible"]} == {str(own["call_id"])}
    stored = await db.pool.fetchrow("SELECT * FROM calls WHERE id=$1", private["call_id"])
    assert stored["prompt"] is stored["response"] is stored["embedding"] is None
    assert float(stored["cost"]) == 0.01
    assert await db.health() and await graph.health()


async def test_services_graph_blocks_foreign_and_expired_intermediate_nodes(services):
    _, graph, tenant, other = services
    seed, neighbor, hidden, expired_hidden = [graph_arguments(tenant) for _ in range(4)]
    foreign = graph_arguments(other)
    expired_bridge = graph_arguments(tenant)
    for args in (seed, neighbor, hidden, expired_hidden, foreign, expired_bridge):
        await graph.write_call(**args)
    async with graph.driver.session() as session:
        # Deliberately corrupt edge scopes: traversal must also check every node.
        edges = [
            [seed["call_id"], neighbor["call_id"]],
            [seed["call_id"], foreign["call_id"]], [foreign["call_id"], hidden["call_id"]],
            [seed["call_id"], expired_bridge["call_id"]],
            [expired_bridge["call_id"], expired_hidden["call_id"]],
        ]
        result = await session.run("""
            UNWIND $edges AS edge
            MATCH (a:GatewayCall {id:edge[0]}), (b:GatewayCall {id:edge[1]})
            MERGE (a)-[r:SIMILAR_TO]->(b)
            SET r.tenant_id=$tenant, r.user_id='reader', r.feature_tag='research',
                r.expires_at=datetime()+duration('P1D')
        """, edges=edges, tenant=tenant)
        await result.consume()
        result = await session.run(
            "MATCH (c:GatewayCall {id:$id}) SET c.expires_at=datetime()-duration('PT1S')",
            id=expired_bridge["call_id"],
        )
        await result.consume()
    hits = await graph.expand_neighborhood(
        [seed["call_id"]], 100, tenant_id=tenant, user_id="reader", feature_tag="research"
    )
    assert {row["id"] for row in hits} == {seed["call_id"], neighbor["call_id"]}
    assert (await graph.stats(tenant_id=other))["calls"] == 1
    assert (await graph.stats(tenant_id=tenant, user_id="different"))["calls"] == 0


async def test_services_outbox_restart_lease_fencing_and_ordered_dependencies(services):
    db, graph, tenant, _ = services
    first = call_arguments(tenant)
    second = call_arguments(tenant)
    for args, informed_by in ((first, []), (second, [str(first["call_id"])])):
        payload = graph_arguments(tenant, call_id=str(args["call_id"]), informed_by=informed_by)
        await db.log_call(**{**args, "graph_event": {"kind": "call", "payload": payload}})
        await db.log_call(**{**args, "graph_event": {"kind": "call", "payload": payload}})
    # First process dies after claiming: later same-scope calls must remain blocked.
    claimed = await db.claim_graph_events(limit=100)
    claimed = [row for row in claimed if row["call_id"] == first["call_id"]]
    assert len(claimed) == 1
    assert await db.claim_graph_events(limit=100) == []
    await db.pool.execute(
        "UPDATE graph_outbox SET lease_until=now()-interval '1 second' WHERE id=$1", claimed[0]["id"]
    )
    replacement = await db.claim_graph_events(limit=100)
    assert len(replacement) == 1
    assert replacement[0]["lease_token"] != claimed[0]["lease_token"]
    await db.ack_graph_event(claimed[0]["id"], claimed[0]["lease_token"])
    assert await db.pool.fetchval("SELECT count(*) FROM graph_outbox WHERE tenant_id=$1", tenant) == 2
    await db.pool.execute(
        "UPDATE graph_outbox SET lease_until=NULL WHERE tenant_id=$1", tenant
    )
    worker = GraphOutboxWorker(db, graph)
    assert await worker.run_once() == 1
    assert await worker.run_once() == 1
    assert await db.pool.fetchval("SELECT count(*) FROM graph_outbox WHERE tenant_id=$1", tenant) == 0
    counts = await graph.stats(tenant_id=tenant)
    assert counts == {"calls": 2, "similar_edges": 0, "informed_by_edges": 1}
    # Consumer replay is harmless and preserves original creation/expiry.
    await graph.write_call(**graph_arguments(tenant, call_id=str(first["call_id"])))
    assert (await graph.stats(tenant_id=tenant))["calls"] == 2


async def test_services_migrates_legacy_schema_into_quarantine(monkeypatch):
    """Use an isolated schema; never mutate the test stack's public schema."""
    schema = "migration_test_" + uuid.uuid4().hex
    admin = await asyncpg.connect(settings.database_url)
    real_create_pool = asyncpg.create_pool
    db = Database()
    try:
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        await admin.execute(f'SET search_path TO "{schema}", public')
        sql = (Path(__file__).resolve().parents[1] / "migrations" / "001_initial.sql").read_text()
        await admin.execute(sql.replace("{dim}", str(settings.embedding_dim)))
        legacy_id = uuid.uuid4()
        await admin.execute(
            "INSERT INTO calls(id,user_id,feature_tag,prompt,response,embedding) "
            "VALUES ($1,'reader','research','legacy secret','legacy answer',$2::vector)",
            legacy_id, "[" + ",".join(map(str, vector())) + "]",
        )

        async def scoped_pool(*args, **kwargs):
            return await real_create_pool(
                *args, **kwargs, server_settings={"search_path": f'"{schema}",public'}
            )

        monkeypatch.setattr("app.db.asyncpg.create_pool", scoped_pool)
        await db.connect()
        assert await db.pool.fetchval("SELECT tenant_id FROM calls WHERE id=$1", legacy_id) == (
            "__legacy_quarantine__"
        )
        assert await db.find_similar(
            vector(), 10, 0.9, tenant_id="local", user_id="reader", feature_tag="research",
            max_tokens=128, embedding_space="integration-v1",
        ) == []
        assert await db.pool.fetchval("SELECT count(*) FROM gateway_schema_migrations") == 3
        await db.close()
        await db.connect()  # restart does not reapply migrations
        assert await db.pool.fetchval("SELECT count(*) FROM gateway_schema_migrations") == 3
    finally:
        await db.close()
        await admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await admin.close()


async def test_services_physical_retention_purge_preserves_accounting(services):
    db, graph, tenant, _ = services
    args = call_arguments(tenant)
    payload = graph_arguments(tenant, call_id=str(args["call_id"]))
    await db.log_call(**{**args, "graph_event": {"kind": "call", "payload": payload}})
    await graph.write_call(**payload)
    await db.pool.execute(
        "UPDATE calls SET expires_at=now()-interval '1 second' WHERE id=$1", args["call_id"]
    )
    async with graph.driver.session() as session:
        result = await session.run(
            "MATCH (c:GatewayCall {id:$id}) SET c.expires_at=datetime()-duration('PT1S')",
            id=str(args["call_id"]),
        )
        await result.consume()
    assert await db.purge_expired_memory() >= 1
    assert await graph.purge_expired_memory() >= 1
    row = await db.pool.fetchrow("SELECT * FROM calls WHERE id=$1", args["call_id"])
    assert row["prompt"] is row["response"] is row["embedding"] is None
    assert float(row["cost"]) == 0.01
    assert await db.pool.fetchval("SELECT count(*) FROM graph_outbox WHERE tenant_id=$1", tenant) == 0
    async with graph.driver.session() as session:
        result = await session.run("MATCH (n {tenant_id:$tenant}) RETURN count(n) AS n", tenant=tenant)
        assert (await result.single())["n"] == 0


async def test_services_outbox_projects_independent_scopes_in_parallel(services, monkeypatch):
    db, graph, tenant, _ = services
    monkeypatch.setattr(settings, "graph_outbox_concurrency", 2)
    first, dependent, independent = (
        call_arguments(tenant), call_arguments(tenant), call_arguments(tenant, user_id="another-reader")
    )
    for args in (first, dependent, independent):
        payload = graph_arguments(tenant, call_id=str(args["call_id"]), user_id=args["user_id"])
        await db.log_call(**{**args, "graph_event": {"kind": "call", "payload": payload}})
    entered = set()
    simultaneous = asyncio.Event()
    release = asyncio.Event()
    write_call = graph.write_call

    async def gated_write(**kwargs):
        entered.add(kwargs["call_id"])
        if len(entered) == 2:
            simultaneous.set()
        await release.wait()
        await write_call(**kwargs)

    monkeypatch.setattr(graph, "write_call", gated_write)
    worker = GraphOutboxWorker(db, graph)
    task = asyncio.create_task(worker.run_once())
    try:
        await asyncio.wait_for(simultaneous.wait(), timeout=2)
        assert entered == {str(first["call_id"]), str(independent["call_id"])}
    finally:
        release.set()
        await task
    assert task.result() == 2
    assert await db.pool.fetchval("SELECT count(*) FROM graph_outbox WHERE tenant_id=$1", tenant) == 1
    assert await worker.run_once() == 1
    assert (await graph.stats(tenant_id=tenant))["calls"] == 3

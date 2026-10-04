"""Real PostgreSQL/Neo4j lifecycle, revision and stale-replay tests in isolated scopes."""

import asyncio
import json
import os
import uuid
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio

from app.config import settings
from app.db import Database, MemoryConflict, MemoryNotFound
from app.graph import MemoryGraph
from app.outbox import GraphOutboxWorker

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_SERVICE_TESTS") != "1",
        reason="RUN_SERVICE_TESTS=1 required",
    ),
]


def vector():
    return [1.0] + [0.0] * (settings.embedding_dim - 1)


@pytest_asyncio.fixture
async def memory_services():
    schema = "memory_lifecycle_" + uuid.uuid4().hex
    tenant = "memory-lifecycle-" + uuid.uuid4().hex
    admin = await asyncpg.connect(settings.database_url)
    db, graph = Database(), MemoryGraph()
    await admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        await admin.execute(f'SET search_path TO "{schema}",public')
        for path in sorted((Path(__file__).resolve().parents[1] / "migrations").glob("[0-9]*.sql")):
            await admin.execute(path.read_text().replace("{dim}", str(settings.embedding_dim)))
        db.pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=1,
            max_size=4,
            server_settings={"search_path": f'"{schema}",public'},
        )
        await graph.connect()
        yield db, graph, {"tenant_id": tenant, "user_id": "reader", "feature_tag": "support"}
    finally:
        if graph.driver:
            async with graph.driver.session() as session:
                result = await session.run(
                    "MATCH (n) WHERE n.tenant_id STARTS WITH $tenant DETACH DELETE n", tenant=tenant
                )
                await result.consume()
        await graph.close()
        await db.close()
        await admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await admin.close()


async def curated(db, scope, prompt="verified fact", response="approved answer"):
    return await db.create_memory(
        **scope,
        prompt=prompt,
        response=response,
        embedding=vector(),
        embedding_space="memory-test",
        expected_scope_revision=await db.memory_epoch(**scope),
    )


def call(scope, **overrides):
    return {
        **scope,
        "call_id": uuid.uuid4(),
        "prompt": "follow-up",
        "response": "derived answer",
        "model": "test-model",
        "provider": "test-provider",
        "tokens_in": 3,
        "tokens_out": 2,
        "cost": 0.125,
        "latency_ms": 7,
        "cache_hit": False,
        "fallback_used": False,
        "embedding": vector(),
        "embedding_space": "memory-test",
        "generation_config": "policy",
        "max_tokens": 128,
        "source_ids": [],
        **overrides,
    }


async def test_correction_erases_derived_content_preserving_scope_and_accounting(memory_services):
    db, _, scope = memory_services
    first = await curated(db, scope)
    old_id = uuid.UUID(first["memory"]["id"])
    independent = await curated(db, scope, "independent fact", "independent answer")
    foreign = await curated(db, {**scope, "user_id": "another-user"})
    derived = call(scope, source_ids=[str(old_id)])
    await db.log_call(**derived, expected_memory_epoch=await db.memory_epoch(**scope))
    descendant = call(scope, source_ids=[str(derived["call_id"])], prompt="follow-up again")
    await db.log_call(**descendant, expected_memory_epoch=await db.memory_epoch(**scope))
    corrected = await db.create_memory(
        **scope,
        prompt="corrected fact",
        response="corrected answer",
        embedding=vector(),
        embedding_space="memory-test",
        supersedes_id=old_id,
        expected_revision=1,
    )
    assert corrected["memory"]["supersedes_id"] == str(old_id)
    assert corrected["invalidated_count"] == 2
    for memory_id, status in (
        (old_id, "superseded"),
        (derived["call_id"], "invalidated"),
        (descendant["call_id"], "invalidated"),
    ):
        record = await db.memory_detail(memory_id, **scope)
        assert record["status"] == status and record["revision"] == 2
        assert record["prompt"] is record["response"] is None
        row = await db.pool.fetchrow("SELECT embedding,cost FROM calls WHERE id=$1", memory_id)
        assert row["embedding"] is None
        if memory_id != old_id:
            assert float(row["cost"]) == 0.125
    assert (await db.memory_detail(uuid.UUID(independent["memory"]["id"]), **scope))["status"] == "active"
    with pytest.raises(MemoryNotFound):
        await db.memory_detail(uuid.UUID(foreign["memory"]["id"]), **scope)
    assert await db.filter_active_memories([str(old_id), str(derived["call_id"])], **scope) == []
    with pytest.raises(MemoryConflict):
        await db.delete_memory(old_id, **scope, expected_revision=1)
    payloads = await db.pool.fetch("SELECT event FROM graph_outbox WHERE call_id=$1", old_id)
    assert len(payloads) == 1 and json.loads(payloads[0]["event"])["kind"] == "delete"
    assert "verified fact" not in payloads[0]["event"]


async def test_scope_delete_invalidates_cache_and_inflight_write_preserves_only_accounting(memory_services):
    db, _, scope = memory_services
    await curated(db, scope)
    cached = call(scope, prompt="repeat exactly")
    epoch = await db.memory_epoch(**scope)
    await db.log_call(**cached, expected_memory_epoch=epoch)
    exact_args = {**scope, "embedding_space": "memory-test", "generation_config": "policy", "max_tokens": 128}
    assert await db.find_exact("repeat exactly", **exact_args)
    foreign_scope = {**scope, "tenant_id": scope["tenant_id"] + "-foreign"}
    await curated(db, foreign_scope)
    deleted = await db.delete_memory(None, **scope, expected_revision=epoch)
    assert deleted["deleted_count"] == 2 and deleted["scope_revision"] == epoch + 1
    assert await db.find_exact("repeat exactly", **exact_args) is None
    stale = call(scope, source_ids=[str(cached["call_id"])])
    with pytest.raises(MemoryConflict):
        await db.log_call(**stale, expected_memory_epoch=epoch)
    row = await db.pool.fetchrow("SELECT * FROM calls WHERE id=$1", stale["call_id"])
    assert row["prompt"] is row["response"] is row["embedding"] is None
    assert not row["memory_visible"] and float(row["cost"]) == 0.125
    assert await db.pool.fetchval("SELECT count(*) FROM graph_outbox WHERE call_id=$1", stale["call_id"]) == 0
    assert len((await db.list_memories(**foreign_scope))["items"]) == 1


async def test_stale_leased_graph_events_cannot_resurrect_deleted_content(memory_services):
    db, graph, scope = memory_services
    seed = await curated(db, scope)
    memory_id = uuid.UUID(seed["memory"]["id"])
    leased = await db.claim_graph_events()
    assert len(leased) == 1
    old_payload = leased[0]["event"]["payload"]
    await db.delete_memory(memory_id, **scope, expected_revision=1)
    worker = GraphOutboxWorker(db, graph)
    assert await worker.run_once() == 1
    # The old worker already has a payload in RAM and finishes after deletion.
    await graph.write_call(**old_payload)
    await db.ack_graph_event(leased[0]["id"], leased[0]["lease_token"])
    assert (await graph.stats(**scope))["calls"] == 0
    assert await graph.expand_neighborhood([str(memory_id)], 20, **scope) == []
    async with graph.driver.session() as session:
        result = await session.run(
            "MATCH (c:GatewayCall {id:$id,tenant_id:$tenant}) RETURN properties(c) AS c",
            id=str(memory_id),
            tenant=scope["tenant_id"],
        )
        properties = (await result.single())["c"]
        assert properties["memory_status"] == "deleted" and properties["memory_revision"] == 2
        assert "prompt" not in properties and "response" not in properties
    assert await db.outbox_pending() == 0


async def test_pagination_scope_binding_and_revision_conflicts(memory_services):
    db, _, scope = memory_services
    for index in range(4):
        await curated(db, scope, f"fact {index}", "answer")
    first = await db.list_memories(**scope, limit=2)
    second = await db.list_memories(**scope, limit=2, cursor=first["next_cursor"])
    assert second["next_cursor"] is None
    assert len({row["id"] for row in first["items"] + second["items"]}) == 4
    with pytest.raises(ValueError):
        await db.list_memories(**{**scope, "feature_tag": "other"}, cursor=first["next_cursor"])
    await db.delete_memory(uuid.UUID(first["items"][0]["id"]), **scope, expected_revision=1)
    with pytest.raises(MemoryConflict):
        await db.list_memories(**scope, cursor=first["next_cursor"])
    with pytest.raises(MemoryConflict):
        await db.create_memory(
            **scope,
            prompt="late",
            response="late",
            embedding=vector(),
            embedding_space="memory-test",
            expected_scope_revision=0,
        )


async def test_scope_lock_serializes_mutation_against_inflight_generation(memory_services):
    db, _, scope = memory_services
    await curated(db, scope)
    epoch = await db.memory_epoch(**scope)
    stale = call(scope)
    async with db.pool.acquire() as conn:
        async with conn.transaction():
            await db._lock_scope(conn, **scope)
            pending = asyncio.create_task(db.log_call(**stale, expected_memory_epoch=epoch))
            await asyncio.sleep(0.02)
            assert not pending.done()
            await db._retire_memories(conn, **scope)
            await db._advance_scope(conn, **scope)
    with pytest.raises(MemoryConflict):
        await pending
    assert await db.pool.fetchval("SELECT response FROM calls WHERE id=$1", stale["call_id"]) is None


async def test_curated_nodes_get_similarity_edges_without_claiming_generated_provenance(memory_services):
    db, graph, scope = memory_services
    await curated(db, scope, "first fact", "first answer")
    await curated(db, scope, "second fact", "second answer")
    worker = GraphOutboxWorker(db, graph)
    assert await worker.run_once() == 1
    assert await worker.run_once() == 1
    counts = await graph.stats(**scope)
    assert counts["calls"] == 2 and counts["similar_edges"] == 1 and counts["informed_by_edges"] == 0


async def test_graph_revision_fence_wins_concurrent_old_writes_and_deletion(memory_services):
    db, graph, scope = memory_services
    seed = await curated(db, scope)
    event = (await db.claim_graph_events())[0]["event"]["payload"]
    deletion = {**scope, "call_id": seed["memory"]["id"], "memory_revision": 2}
    await asyncio.gather(
        *[graph.write_call(**event) if index % 2 else graph.delete_call(**deletion) for index in range(12)]
    )
    assert (await graph.stats(**scope))["calls"] == 0
    async with graph.driver.session() as session:
        result = await session.run(
            "MATCH(c:GatewayCall {id:$id,tenant_id:$tenant}) "
            "RETURN c.memory_revision AS revision,c.prompt AS prompt",
            id=seed["memory"]["id"],
            tenant=scope["tenant_id"],
        )
        record = await result.single()
        assert record["revision"] == 2 and record["prompt"] is None


async def test_authenticated_memory_http_create_correct_inspect_delete_without_generation(
    memory_services, monkeypatch
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    import httpx

    from app.main import create_app

    db, graph, boundary = memory_services
    key = "memory-api-test-only-" + "x" * 32
    monkeypatch.setattr(settings, "gateway_api_keys", {key: boundary["tenant_id"]})
    monkeypatch.setattr(settings, "auth_enabled", True)
    app = create_app()
    app.state.ready = True
    app.state.db, app.state.graph = db, graph
    app.state.limiter = SimpleNamespace(check=AsyncMock(return_value=(True, 0)))
    app.state.embedder = SimpleNamespace(space_id="memory-test", embed=AsyncMock(return_value=vector()))
    app.state.router = SimpleNamespace(
        complete=AsyncMock(side_effect=AssertionError("No generation permitted"))
    )
    scope = {"user_id": boundary["user_id"], "feature_tag": boundary["feature_tag"]}
    headers = {"Authorization": "Bearer " + key}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://memory") as client:
        assert (await client.get("/v1/memories", params=scope)).status_code == 401
        assert (await client.get("/v1/memories", headers=headers)).status_code == 422
        initial = await client.get("/v1/memories", params=scope, headers=headers)
        assert initial.json()["scope_revision"] == 0
        body = {**scope, "prompt": "known fact", "response": "known answer", "expected_scope_revision": 0}
        assert (
            await client.post("/v1/memories", json={**body, "tenant_id": "spoof"}, headers=headers)
        ).status_code == 422
        created = await client.post("/v1/memories", json=body, headers=headers)
        assert created.status_code == 201, created.text
        original = created.json()["memory"]
        assert original["revision"] == 1 and original["cache_eligible"] is False
        assert (await client.post("/v1/memories", json=body, headers=headers)).status_code == 409
        assert app.state.embedder.embed.await_count == 1
        corrected_body = {
            **scope,
            "prompt": "correct fact",
            "response": "correct answer",
            "expected_revision": 1,
        }
        assert (
            await client.patch(
                f"/v1/memories/{original['id']}", json={**corrected_body, "user_id": "other"}, headers=headers
            )
        ).status_code == 404
        assert app.state.embedder.embed.await_count == 1
        corrected = await client.patch(f"/v1/memories/{original['id']}", json=corrected_body, headers=headers)
        assert corrected.status_code == 200, corrected.text
        replacement = corrected.json()["memory"]
        assert replacement["supersedes_id"] == original["id"]
        detail = await client.get(f"/v1/memories/{original['id']}", params=scope, headers=headers)
        assert detail.json()["status"] == "superseded" and detail.json()["response"] is None
        deleted = await client.delete(
            f"/v1/memories/{replacement['id']}", params={**scope, "expected_revision": 1}, headers=headers
        )
        assert deleted.status_code == 200 and deleted.json()["deleted_count"] == 1
        listing = await client.get("/v1/memories", params=scope, headers=headers)
        assert len(listing.json()["items"]) == 2
        assert all(row["prompt"] is row["response"] is None for row in listing.json()["items"])
        app.state.router.complete.assert_not_called()
        assert all(call.kwargs["cache"] is False for call in app.state.embedder.embed.await_args_list)


async def test_targeted_delete_invalidates_other_completion_cache_without_erasing_independent_evidence(
    memory_services,
):
    db, _, scope = memory_services
    source = await curated(db, scope)
    independent = call(scope, prompt="independent repeat", source_ids=[])
    await db.log_call(**independent, expected_memory_epoch=await db.memory_epoch(**scope))
    args = {**scope, "embedding_space": "memory-test", "generation_config": "policy", "max_tokens": 128}
    assert await db.find_exact("independent repeat", **args)
    await db.delete_memory(uuid.UUID(source["memory"]["id"]), **scope, expected_revision=1)
    assert await db.find_exact("independent repeat", **args) is None
    hydrated = await db.filter_active_memories([str(independent["call_id"])], **scope)
    assert len(hydrated) == 1 and hydrated[0]["response"] == "derived answer"


async def test_expired_memory_is_not_disclosed_by_inspection_before_physical_cleanup(memory_services):
    db, _, scope = memory_services
    created = await curated(db, scope)
    memory_id = uuid.UUID(created["memory"]["id"])
    await db.pool.execute("UPDATE calls SET expires_at=now()-interval '1 second' WHERE id=$1", memory_id)
    detail = await db.memory_detail(memory_id, **scope)
    assert detail["status"] == "expired" and detail["prompt"] is detail["response"] is None
    listed = (await db.list_memories(**scope))["items"][0]
    assert listed["status"] == "expired" and listed["response"] is None


async def test_derived_lifetime_and_graph_payload_cannot_outlive_source(memory_services):
    db, _, scope = memory_services
    seed = await curated(db, scope)
    source_id = uuid.UUID(seed["memory"]["id"])
    expiry = await db.pool.fetchval(
        "UPDATE calls SET expires_at=clock_timestamp()+interval '30 seconds' "
        "WHERE id=$1 RETURNING expires_at",
        source_id,
    )
    derived = call(scope, source_ids=[str(source_id)], graph_event={"kind": "call", "payload": {}})
    await db.log_call(**derived, expected_memory_epoch=await db.memory_epoch(**scope))
    row = await db.pool.fetchrow(
        "SELECT expires_at,cache_expires_at FROM calls WHERE id=$1", derived["call_id"]
    )
    assert row["expires_at"] == row["cache_expires_at"] == expiry
    event = json.loads(
        await db.pool.fetchval("SELECT event FROM graph_outbox WHERE call_id=$1", derived["call_id"])
    )
    from datetime import datetime

    assert datetime.fromisoformat(event["payload"]["expires_at"]) == expiry
    await db.pool.execute(
        "UPDATE calls SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", source_id
    )
    expired_source_call = call(scope, source_ids=[str(source_id)])
    with pytest.raises(MemoryConflict):
        await db.log_call(**expired_source_call, expected_memory_epoch=await db.memory_epoch(**scope))
    accounting = await db.pool.fetchrow(
        "SELECT prompt,response,cost FROM calls WHERE id=$1", expired_source_call["call_id"]
    )
    assert accounting["prompt"] is accounting["response"] is None and float(accounting["cost"]) == 0.125


async def test_reused_call_id_cannot_bypass_changed_scope_epoch(memory_services):
    db, _, scope = memory_services
    original = call(scope)
    await db.log_call(**original, expected_memory_epoch=0)
    await curated(db, scope)
    with pytest.raises(MemoryConflict):
        await db.log_call(**original, expected_memory_epoch=0)
    await db.delete_memory(original["call_id"], **scope, expected_revision=1)
    with pytest.raises(MemoryConflict):
        await db.log_call(**original)
    assert await db.pool.fetchval("SELECT response FROM calls WHERE id=$1", original["call_id"]) is None
    assert "derived answer" not in await db.pool.fetchval(
        "SELECT string_agg(event::text,'') FROM graph_outbox WHERE call_id=$1",
        original["call_id"],
    )

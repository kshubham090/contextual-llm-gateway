"""Console truthfulness, authorization and graph retirement against real stores."""

import asyncio
import json
import os
import uuid
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from app.config import settings
from app.graph import _key
from app.main import create_app
from app.outbox import GraphOutboxWorker
from tests.test_memory_services import call, curated
from tests.test_memory_services import memory_services as memory_services

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_SERVICE_TESTS") != "1", reason="RUN_SERVICE_TESTS=1 required"),
]


def console_app(db, graph, scope, monkeypatch):
    key = "console-test-only-" + "x" * 32
    monkeypatch.setattr(settings, "gateway_api_keys", {key: scope["tenant_id"]})
    monkeypatch.setattr(settings, "auth_enabled", True)
    app = create_app()
    app.state.ready = True
    app.state.db, app.state.graph = db, graph
    app.state.limiter = SimpleNamespace(
        check=AsyncMock(return_value=(True, 0)),
        health=AsyncMock(return_value=True),
    )
    app.state.embedder = SimpleNamespace(
        space_id="synthetic-fixture:1024:v1",
        health=lambda: {"started": True, "closed": False},
    )
    app.state.router = SimpleNamespace(provider=SimpleNamespace(name="synthetic-demo"))
    return (
        app,
        {"Authorization": "Bearer " + key},
        {
            "user_id": scope["user_id"],
            "feature_tag": scope["feature_tag"],
        },
    )


async def project_all(db, graph):
    worker = GraphOutboxWorker(db, graph)
    for _ in range(20):
        if not await worker.run_once():
            assert await db.outbox_pending() == 0
            return
    raise AssertionError("Test outbox did not drain")


async def test_console_accounting_includes_private_requests_not_seeds_or_other_scopes(memory_services):
    db, _, scope = memory_services
    await curated(db, scope, "sensitive active fact")
    retired = await curated(db, scope, "sensitive retired fact")
    await db.delete_memory(uuid.UUID(retired["memory"]["id"]), **scope, expected_revision=1)
    recorded = [
        call(
            scope, prompt="sensitive generated prompt", cost=0.25, tokens_in=10, tokens_out=4, latency_ms=10
        ),
        call(
            scope,
            prompt=None,
            response=None,
            embedding=None,
            cost=None,
            tokens_in=20,
            tokens_out=5,
            latency_ms=50,
        ),
        call(scope, cache_hit=True, embedding=None, cost=0, tokens_in=0, tokens_out=0, latency_ms=2),
        call(scope, prompt="sensitive expired prompt", cost=0.75, tokens_in=30, tokens_out=6, latency_ms=30),
    ]
    for item in recorded:
        await db.log_call(**item, expected_memory_epoch=await db.memory_epoch(**scope))
    await db.delete_memory(recorded[0]["call_id"], **scope, expected_revision=1)
    await db.pool.execute(
        "UPDATE calls SET expires_at=now()-interval '1 second' WHERE id=$1", recorded[3]["call_id"]
    )
    await db.pool.execute(
        "UPDATE calls SET created_at=now()-interval '1 day' WHERE id=$1", recorded[1]["call_id"]
    )
    old = call(scope, prompt=None, embedding=None, cost=99)
    await db.log_call(**old)
    now = await db.pool.fetchval("SELECT now()")
    start = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=6)
    await db.pool.execute(
        "UPDATE calls SET created_at=$2 WHERE id=$1", old["call_id"], start - timedelta(seconds=1)
    )
    for foreign in (
        {**scope, "tenant_id": scope["tenant_id"] + "-foreign"},
        {**scope, "user_id": "another-user"},
        {**scope, "feature_tag": "another-feature"},
    ):
        await db.log_call(**call(foreign, cost=99))
    result = await db.console_overview(**scope)
    assert result["totals"] == {
        "calls": 4,
        "cache_hits": 1,
        "tokens_in": 60,
        "tokens_out": 15,
        "known_cost": 1.0,
        "unpriced_calls": 1,
        "mean_latency_ms": 23.0,
        "p95_latency_ms": pytest.approx(47.0),
    }
    assert len(result["daily"]) == 7 and sum(day["calls"] for day in result["daily"]) == 4
    assert result["daily"][-2]["unpriced_calls"] == 1
    assert result["memory"] == {
        "total_count": 4,
        "active_count": 1,
        "retired_count": 2,
        "expired_count": 1,
        "curated_active_count": 1,
    }
    assert {row["id"] for row in result["recent_requests"]} == {str(item["call_id"]) for item in recorded}
    private = next(row for row in result["recent_requests"] if row["id"] == str(recorded[1]["call_id"]))
    assert private["cost"] is None and not private["retained_content"]
    serialized = json.dumps(result)
    assert "sensitive" not in serialized and '"prompt"' not in serialized and '"response"' not in serialized


async def test_console_graph_contains_only_real_scoped_edges_and_live_previews(memory_services, monkeypatch):
    db, graph, scope = memory_services
    first = (await curated(db, scope, "first live fact"))["memory"]["id"]
    second = (await curated(db, scope, "second live fact"))["memory"]["id"]
    foreign = (await curated(db, {**scope, "user_id": "other"}, "foreign secret"))["memory"]["id"]
    generated = call(scope, prompt="derived from first fact", source_ids=[first])
    payload = {
        name: generated[name]
        for name in (
            "user_id",
            "feature_tag",
            "prompt",
            "response",
            "model",
            "provider",
            "tokens_in",
            "tokens_out",
            "cost",
            "latency_ms",
        )
    }
    payload.update(fallback_provider=None, similar=[], informed_by=[first])
    await db.log_call(
        **generated,
        graph_event={"kind": "call", "payload": payload},
        expected_memory_epoch=await db.memory_epoch(**scope),
    )
    await db.log_call(**call(scope, prompt=None, embedding=None))
    await db.log_call(**call(scope, cache_hit=True, embedding=None))
    third = str(generated["call_id"])
    await project_all(db, graph)
    async with graph.driver.session() as session:
        await session.run(
            "MATCH (a:GatewayCall {id:$first}),(b:GatewayCall {id:$second}) "
            "CREATE (a)-[:INFORMED_BY {tenant_id:$tenant,user_id:'other',feature_tag:$feature,"
            "expires_at:datetime()+duration('P1D')}]->(b)",
            first=first,
            second=second,
            tenant=scope["tenant_id"],
            feature=scope["feature_tag"],
        )
    # Even if a caller includes a foreign ID, graph endpoint and edge scopes are checked.
    raw = await graph.console_edges([first, second, third, foreign], **scope)
    assert len(raw["edges"]) == 2
    assert {edge["type"] for edge in raw["edges"]} == {"SIMILAR_TO", "INFORMED_BY"}
    app, headers, params = console_app(db, graph, scope, monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://console") as client:
        response = await client.get("/v1/console/graph", params=params, headers=headers)
        assert response.status_code == 200
        result = response.json()
        assert {node["id"] for node in result["nodes"]} == {first, second, third}
        assert {edge["type"]: edge for edge in result["edges"]} == {
            "SIMILAR_TO": {
                "source": second,
                "target": first,
                "type": "SIMILAR_TO",
                "similarity": pytest.approx(1.0),
            },
            "INFORMED_BY": {"source": third, "target": first, "type": "INFORMED_BY", "similarity": None},
        }
        assert not result["degraded"] and "foreign secret" not in response.text
        # Deliberately do not project the tombstone: PG retirement wins immediately.
        await db.delete_memory(uuid.UUID(first), **scope, expected_revision=1)
        after = (await client.get("/v1/console/graph", params=params, headers=headers)).json()
        assert [node["id"] for node in after["nodes"]] == [second] and after["edges"] == []
        assert "first live fact" not in json.dumps(after)


async def test_console_graph_bounds_nodes_and_actual_dense_relationships(memory_services, monkeypatch):
    db, graph, scope = memory_services
    records = [call(scope, prompt="fact " + str(index)) for index in range(83)]
    for record in records:
        await db.log_call(**record)
    ids = [str(record["call_id"]) for record in records[-20:]]
    nodes = [
        {"id": value, "key": _key(scope["tenant_id"], scope["user_id"], scope["feature_tag"], value)}
        for value in ids
    ]
    async with graph.driver.session() as session:
        result = await session.run(
            "UNWIND $nodes AS input CREATE (c:GatewayCall) SET c=input, "
            "c.tenant_id=$tenant_id,c.user_id=$user_id,c.feature_tag=$feature_tag,"
            "c.memory_status='active',c.expires_at=datetime()+duration('P1D') "
            "WITH collect(c) AS nodes UNWIND nodes AS a UNWIND nodes AS b "
            "WITH a,b WHERE a<>b CREATE (a)-[r:SIMILAR_TO]->(b) "
            "SET r.tenant_id=$tenant_id,r.user_id=$user_id,r.feature_tag=$feature_tag,"
            "r.score=0.9,r.expires_at=datetime()+duration('P1D')",
            nodes=nodes,
            **scope,
        )
        await result.consume()
    app, headers, params = console_app(db, graph, scope, monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://console") as client:
        response = await client.get("/v1/console/graph", params={**params, "limit": 80}, headers=headers)
        result = response.json()
        assert response.status_code == 200 and not result["degraded"]
        assert len(result["nodes"]) == 80 and result["total_active_nodes"] == 83
        assert len(result["edges"]) == 320 and result["truncated"] == {"nodes": True, "edges": True}
        assert all(edge["source"] in ids and edge["target"] in ids for edge in result["edges"])


async def test_console_graph_rechecks_epoch_after_a_delayed_projection(memory_services, monkeypatch):
    db, graph, scope = memory_services
    first = (await curated(db, scope, "must disappear"))["memory"]["id"]
    entered, release = asyncio.Event(), asyncio.Event()

    async def delayed(*args, **kwargs):
        entered.set()
        await release.wait()
        return {"edges": [], "truncated": False}

    monkeypatch.setattr(graph, "console_edges", delayed)
    app, headers, params = console_app(db, graph, scope, monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://console") as client:
        request = asyncio.create_task(client.get("/v1/console/graph", params=params, headers=headers))
        await asyncio.wait_for(entered.wait(), 2)
        await db.delete_memory(uuid.UUID(first), **scope, expected_revision=1)
        release.set()
        response = await request
        assert response.status_code == 409 and "must disappear" not in response.text


async def test_console_graph_expiry_and_projection_outage_are_honest(memory_services, monkeypatch):
    db, graph, scope = memory_services
    first = (await curated(db, scope, "naturally expires"))["memory"]["id"]
    second = (await curated(db, scope, "still active"))["memory"]["id"]

    async def expire_while_loading(*args, **kwargs):
        await db.pool.execute(
            "UPDATE calls SET expires_at=now()-interval '1 second' WHERE id=$1", uuid.UUID(first)
        )
        return {
            "edges": [{"source": first, "target": second, "type": "SIMILAR_TO", "similarity": 1}],
            "truncated": False,
        }

    monkeypatch.setattr(graph, "console_edges", expire_while_loading)
    app, headers, params = console_app(db, graph, scope, monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://console") as client:
        result = (await client.get("/v1/console/graph", params=params, headers=headers)).json()
        assert [node["id"] for node in result["nodes"]] == [second] and result["edges"] == []
        assert result["total_active_nodes"] == 1
        monkeypatch.setattr(graph, "console_edges", AsyncMock(side_effect=RuntimeError("SECRET URL")))
        response = await client.get("/v1/console/graph", params=params, headers=headers)
        result = response.json()
        assert response.status_code == 200 and result["degraded"] and result["edges"] == []
        assert result["nodes"][0]["prompt_preview"] == "still active" and "SECRET" not in response.text


async def test_console_auth_scope_limits_and_safe_runtime(memory_services, monkeypatch):
    db, graph, scope = memory_services
    await curated(db, scope, "private preview")
    await curated(db, scope, "second private preview")
    app, headers, params = console_app(db, graph, scope, monkeypatch)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://console") as client:
        for endpoint in ("overview", "graph"):
            assert (await client.get(f"/v1/console/{endpoint}", params=params)).status_code == 401
            assert (await client.get(f"/v1/console/{endpoint}", headers=headers)).status_code == 422
        assert (
            await client.get("/v1/console/graph", params={**params, "limit": 81}, headers=headers)
        ).status_code == 422
        limited = (
            await client.get("/v1/console/graph", params={**params, "limit": 1}, headers=headers)
        ).json()
        assert len(limited["nodes"]) == 1 and limited["truncated"]["nodes"]
        empty = (
            await client.get(
                "/v1/console/overview", params={**params, "feature_tag": "unknown"}, headers=headers
            )
        ).json()
        assert empty["totals"]["calls"] == 0 and empty["memory"]["total_count"] == 0
        assert empty["runtime"]["generation"]["backend"] == "synthetic-demo"
        assert empty["runtime"]["embedding"]["backend"] == "synthetic-fixture"
        assert empty["runtime"]["generation"]["default_max_tokens"] == settings.default_max_tokens
        assert empty["health"] == {"postgres": True, "redis": True, "graph": True, "embedding": True}
        app.state.limiter.check.return_value = False, 13
        denied = await client.get("/v1/console/overview", params=params, headers=headers)
        assert denied.status_code == 429 and denied.headers["retry-after"] == "13"
        app.state.limiter.check.assert_called_with(scope["user_id"], tenant_id=scope["tenant_id"])

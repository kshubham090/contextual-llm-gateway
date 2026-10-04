"""Public HTTP workflow with real stores and labeled synthetic inference."""

import json
import os
import uuid

import httpx
import pytest

from app.config import settings
from scripts.demo_server import build_demo_app

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_SERVICE_TESTS") != "1", reason="RUN_SERVICE_TESTS=1 required"),
]


async def test_inspector_memory_stream_correction_and_forgetting(monkeypatch):
    tenant = f"workflow-{uuid.uuid4()}"
    token = "workflow-test-token-not-a-secret-0001"
    monkeypatch.setattr(settings, "gateway_api_keys", {token: tenant})
    monkeypatch.setattr(settings, "auth_enabled", True)
    monkeypatch.setattr(settings, "environment", "development")
    monkeypatch.setattr(settings, "rate_limit_per_minute", 1000)
    app = build_demo_app()
    scope = {"user_id": "returning-customer", "feature_tag": "support"}
    async with app.router.lifespan_context(app):
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
                headers={"Authorization": f"Bearer {token}"},
            ) as client:
                shell = await client.get("/inspector")
                assert shell.status_code == 200
                assert "frame-ancestors 'none'" in shell.headers["content-security-policy"]
                assert token not in shell.text
                unauthorized = await client.get(
                    "/v1/memories", params=scope, headers={"Authorization": "Bearer wrong"}
                )
                assert unauthorized.status_code == 401
                initial = (await client.get("/v1/memories", params=scope)).json()
                created = await client.post(
                    "/v1/memories",
                    json={
                        **scope,
                        "expected_scope_revision": initial["scope_revision"],
                        "prompt": "Connector fails after migration",
                        "response": "Refresh cursor, then reconcile. Escalate after two attempts.",
                    },
                )
                assert created.status_code == 201, created.text
                memory = created.json()["memory"]
                await app.state.outbox.run_once()
                streamed = await client.post(
                    "/v1/chat/stream",
                    json={
                        **scope,
                        "prompt": "Connector still fails after migration. What next?",
                        "store": True,
                        "use_cache": False,
                        "retrieval_mode": "graph",
                    },
                )
                assert streamed.status_code == 200, streamed.text
                events = [
                    json.loads(line[6:]) for line in streamed.text.splitlines() if line.startswith("data: ")
                ]
                final = events[-1]
                assert final["meta"]["durable"]
                assert memory["id"] in final["meta"]["context_used"]
                assert "two attempts" in final["response"]
                corrected = await client.patch(
                    f"/v1/memories/{memory['id']}",
                    json={
                        **scope,
                        "expected_revision": memory["revision"],
                        "prompt": memory["prompt"],
                        "response": "Updated: escalate after three attempts.",
                    },
                )
                assert corrected.status_code == 200, corrected.text
                assert corrected.json()["invalidated_count"] == 1
                old = (await client.get(f"/v1/memories/{memory['id']}", params=scope)).json()
                assert old["status"] == "superseded" and old["response"] is None
                derived = (await client.get(f"/v1/memories/{final['meta']['call_id']}", params=scope)).json()
                assert derived["status"] == "invalidated" and derived["prompt"] is None
                private = await client.post(
                    "/v1/chat",
                    json={
                        **scope,
                        "prompt": "What is the updated rule?",
                        "store": False,
                        "use_cache": False,
                        "retrieval_mode": "vector",
                    },
                )
                assert private.status_code == 200
                assert "three attempts" in private.json()["response"]
                assert "two attempts" not in private.json()["response"]
                private_row = await app.state.db.pool.fetchrow(
                    "SELECT prompt,response,embedding FROM calls WHERE id=$1",
                    uuid.UUID(private.json()["meta"]["call_id"]),
                )
                assert all(value is None for value in private_row.values())
                foreign = await client.get(
                    f"/v1/memories/{memory['id']}", params={**scope, "feature_tag": "another-feature"}
                )
                assert foreign.status_code == 404
                deleted = await client.delete(
                    "/v1/memories",
                    params={**scope, "expected_scope_revision": corrected.json()["scope_revision"]},
                )
                assert deleted.status_code == 200, deleted.text
                after = await client.post(
                    "/v1/chat",
                    json={
                        **scope,
                        "prompt": "What is the rule?",
                        "store": False,
                        "use_cache": False,
                        "retrieval_mode": "graph",
                    },
                )
                assert after.json()["meta"]["context_used"] == []
        finally:
            await app.state.db.pool.execute("DELETE FROM calls WHERE tenant_id=$1", tenant)
            await app.state.db.pool.execute("DELETE FROM memory_scopes WHERE tenant_id=$1", tenant)
            async with app.state.graph.driver.session() as session:
                result = await session.run(
                    "MATCH (n) WHERE n.tenant_id=$tenant DETACH DELETE n", tenant=tenant
                )
                await result.consume()

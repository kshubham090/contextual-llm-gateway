"""Full HTTP/lifecycle/outbox smoke test; only inference is deterministic and unpaid."""

import asyncio
import os
import uuid

import httpx
import pytest

from app.config import settings
from app.main import create_app
from app.providers import CompletionResult

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("RUN_SERVICE_TESTS") != "1", reason="RUN_SERVICE_TESTS=1 required"),
]


async def test_authenticated_http_to_durable_stores_and_back(monkeypatch):
    tenant = "http-integration-" + uuid.uuid4().hex
    key = "integration-key-" + uuid.uuid4().hex
    monkeypatch.setattr(settings, "gateway_api_keys", {key: tenant, key + "-other": tenant + "-other"})
    monkeypatch.setattr(settings, "auth_enabled", True)
    monkeypatch.setattr(settings, "graph_outbox_poll_seconds", 0.01)

    class Embedder:
        space_id = "http-integration:v1"

        async def start(self):
            pass

        async def embed(self, text, **kwargs):
            return [1.0] + [0.0] * (settings.embedding_dim - 1)

        async def close(self):
            pass

        def health(self):
            return {"started": True, "closed": False}

    class Router:
        async def complete(self, prompt, system, max_tokens):
            text = "deterministic context answer" if system else "deterministic plain answer"
            return CompletionResult(text, "claude-haiku-4-5", "anthropic", 10, 5, False, None)

        async def close(self):
            pass

    monkeypatch.setattr("app.main.EmbeddingClient", Embedder)
    monkeypatch.setattr("app.main.Router", Router)
    app = create_app()
    headers = {"Authorization": "Bearer " + key}
    body = {"prompt": "Remember orbit rollback fact", "user_id": "reader", "feature_tag": "deploys"}
    async with app.router.lifespan_context(app):
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as http:
                assert (await http.get("/health/ready")).status_code == 200
                seed = await http.post("/v1/chat", json=body, headers=headers)
                assert seed.status_code == 200, seed.text
                seed_id = seed.json()["meta"]["call_id"]
                assert seed.json()["meta"]["memory_write"] == "queued"
                row = await app.state.db.pool.fetchrow("SELECT * FROM calls WHERE id=$1", uuid.UUID(seed_id))
                assert row["tenant_id"] == tenant and row["prompt"] == body["prompt"]
                async with asyncio.timeout(5):
                    while (await app.state.graph.stats(tenant_id=tenant))["calls"] < 1:
                        await asyncio.sleep(0.02)
                answer = await http.post(
                    "/v1/chat",
                    json={**body, "prompt": "Apply rollback fact", "store": False},
                    headers=headers,
                )
                assert answer.status_code == 200, answer.text
                metadata = answer.json()["meta"]
                assert seed_id in metadata["context_used"]
                assert metadata["memory_write"] == "disabled"
                private = await app.state.db.pool.fetchrow(
                    "SELECT * FROM calls WHERE id=$1",
                    uuid.UUID(metadata["call_id"]),
                )
                assert private["prompt"] is private["response"] is private["embedding"] is None
                isolated = await http.post(
                    "/v1/chat",
                    json={**body, "store": False},
                    headers={"Authorization": "Bearer " + key + "-other"},
                )
                assert isolated.status_code == 200 and isolated.json()["meta"]["context_used"] == []
                cached = await http.post("/v1/chat", json={**body, "store": False}, headers=headers)
                assert cached.status_code == 200 and cached.json()["meta"]["cache_hit"] is True
                assert (await http.get("/v1/usage", headers=headers)).json()[0]["calls"] == 3
        finally:
            await app.state.outbox.stop()
            await app.state.db.pool.execute(
                "DELETE FROM calls WHERE tenant_id=ANY($1::text[])", [tenant, tenant + "-other"]
            )
            async with app.state.graph.driver.session() as session:
                result = await session.run(
                    "MATCH (n) WHERE n.tenant_id IN $tenants DETACH DELETE n",
                    tenants=[tenant, tenant + "-other"],
                )
                await result.consume()
    assert app.state.ready is False

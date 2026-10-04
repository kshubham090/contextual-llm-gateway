"""Console operational boundaries independent of external backing services."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest_asyncio

from app.config import settings
from app.main import create_app


@pytest_asyncio.fixture
async def console_client(monkeypatch):
    key = "console-test-" + "a" * 40
    monkeypatch.setattr(settings, "auth_enabled", True)
    monkeypatch.setattr(settings, "gateway_api_keys", {key: "console-tenant"})
    app = create_app()
    app.state.ready = True
    app.state.db = SimpleNamespace(console_overview=AsyncMock(return_value={}))
    app.state.graph = SimpleNamespace(health=AsyncMock(return_value=True))
    app.state.limiter = SimpleNamespace(
        check=AsyncMock(return_value=(True, 0)), health=AsyncMock(return_value=True)
    )
    app.state.router = SimpleNamespace(provider=SimpleNamespace(name="openai-compatible"))
    app.state.embedder = SimpleNamespace(
        space_id="local:test:384:v1", health=lambda: {"started": True, "closed": False}
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://console") as client:
        yield client, app, {"Authorization": "Bearer " + key}


async def test_runtime_is_allowlisted_configuration_without_urls_paths_or_credentials(
    console_client, monkeypatch
):
    client, app, headers = console_client
    monkeypatch.setattr(settings, "embedding_backend", "local")
    monkeypatch.setattr(settings, "local_embedding_model", "/home/private-user/secret-model")
    monkeypatch.setattr(settings, "simple_model", "https://private-provider.example/api?token=SECRET")
    monkeypatch.setattr(settings, "complex_model", "Qwen/Qwen2.5-0.5B-Instruct")
    monkeypatch.setattr(settings, "openai_api_key", "super-secret-credential")
    response = await client.get(
        "/v1/console/overview", params={"user_id": "alice", "feature_tag": "support"}, headers=headers
    )
    assert response.status_code == 200
    result = response.json()
    assert result["runtime"]["generation"]["simple_model"] == "configured model"
    assert result["runtime"]["generation"]["complex_model"] == "Qwen/Qwen2.5-0.5B-Instruct"
    assert result["runtime"]["embedding"]["model"] == "configured model"
    assert result["runtime"]["embedding"]["device"] == settings.local_embedding_device
    assert "SECRET" not in response.text and "private-user" not in response.text
    assert "super-secret-credential" not in response.text and "url" not in json.dumps(result["runtime"])
    assert response.headers["cache-control"] == "no-store"
    app.state.db.console_overview.assert_awaited_once_with(
        tenant_id="console-tenant",
        user_id="alice",
        feature_tag="support",
    )


async def test_console_deadline_cancels_db_operation_and_returns_safe_error(console_client, monkeypatch):
    client, app, headers = console_client
    monkeypatch.setattr(settings, "request_timeout_seconds", 0.02)
    cancelled = asyncio.Event()

    async def blocked(**scope):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    app.state.db.console_overview.side_effect = blocked
    response = await client.get(
        "/v1/console/overview", params={"user_id": "alice", "feature_tag": "support"}, headers=headers
    )
    assert response.status_code == 504 and cancelled.is_set()
    app.state.graph.health.assert_not_called()


async def test_health_probe_failure_does_not_claim_generation_test_or_disclose_exception(console_client):
    client, app, headers = console_client
    app.state.graph.health.side_effect = RuntimeError("secret graph address")
    app.state.limiter.health.side_effect = RuntimeError("secret redis address")
    response = await client.get(
        "/v1/console/overview", params={"user_id": "alice", "feature_tag": "support"}, headers=headers
    )
    assert response.status_code == 200
    assert response.json()["health"] == {"postgres": True, "redis": False, "graph": False, "embedding": True}
    assert "secret" not in response.text

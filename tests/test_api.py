"""Black-box HTTP tests for access boundaries, safe errors, and operational behavior."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
import pytest_asyncio

from app.config import settings
from app.main import create_app
from app.pipeline import RateLimitExceeded
from app.schemas import ChatMetadata, ChatResponse

KEY = "a" * 40
METRICS_KEY = "m" * 40
BODY = {"prompt": "sensitive input", "user_id": "reader", "feature_tag": "research"}
HEADERS = {"Authorization": f"Bearer {KEY}"}


@pytest_asyncio.fixture
async def client(monkeypatch):
    monkeypatch.setattr(settings, "auth_enabled", True)
    monkeypatch.setattr(settings, "gateway_api_keys", {KEY: "tenant-a", "b" * 40: "tenant-b"})
    monkeypatch.setattr(settings, "metrics_bearer_token", METRICS_KEY)
    app = create_app()
    app.state.ready = True
    app.state.pipeline = SimpleNamespace(
        handle_chat=AsyncMock(
            return_value=ChatResponse(
                response="answer",
                meta=ChatMetadata(call_id="test-call"),
            )
        )
    )
    app.state.db = SimpleNamespace(
        usage_rollup=AsyncMock(return_value=[]), health=AsyncMock(return_value=True)
    )
    app.state.graph = SimpleNamespace(
        stats=AsyncMock(return_value={"calls": 1}), health=AsyncMock(return_value=True)
    )
    app.state.limiter = SimpleNamespace(health=AsyncMock(return_value=True))
    app.state.embedder = SimpleNamespace(health=lambda: {"started": True, "closed": False})
    app.state.outbox = SimpleNamespace(health=lambda: {"running": True})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        yield client, app


@pytest.mark.parametrize("path", ["/v1/usage", "/v1/graph/stats", "/metrics"])
async def test_read_endpoints_require_credentials(client, path):
    http, _ = client
    response = await http.get(path)
    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"


async def test_chat_rejects_missing_invalid_and_spoofed_tenant_identity(client):
    http, app = client
    assert (await http.post("/v1/chat", json=BODY)).status_code == 401
    assert (
        await http.post("/v1/chat", json=BODY, headers={"Authorization": "Bearer wrong"})
    ).status_code == 401
    assert (
        await http.post("/v1/chat", json={**BODY, "tenant_id": "tenant-b"}, headers=HEADERS)
    ).status_code == 422
    app.state.pipeline.handle_chat.assert_not_called()


async def test_chat_and_usage_use_tenant_from_credential(client):
    http, app = client
    response = await http.post("/v1/chat", json=BODY, headers={**HEADERS, "X-Request-ID": "attacker"})
    assert response.status_code == 200
    assert app.state.pipeline.handle_chat.call_args.kwargs == {"tenant_id": "tenant-a"}
    assert response.headers["x-request-id"] != "attacker"
    assert response.headers["cache-control"] == "no-store"
    await http.get("/v1/usage?user_id=reader", headers=HEADERS)
    assert app.state.db.usage_rollup.call_args.kwargs["tenant_id"] == "tenant-a"
    await http.get("/v1/graph/stats", headers={"Authorization": "Bearer " + "b" * 40})
    assert app.state.graph.stats.call_args.kwargs["tenant_id"] == "tenant-b"


async def test_errors_do_not_echo_backend_secrets_and_rate_limit_has_retry_after(client):
    http, app = client
    app.state.pipeline.handle_chat.side_effect = RuntimeError("password=SECRET private prompt")
    response = await http.post("/v1/chat", json=BODY, headers=HEADERS)
    assert response.status_code == 503
    assert "SECRET" not in response.text and "private prompt" not in response.text
    app.state.pipeline.handle_chat.side_effect = RateLimitExceeded(17)
    response = await http.post("/v1/chat", json=BODY, headers=HEADERS)
    assert response.status_code == 429 and response.headers["retry-after"] == "17"


async def test_body_limit_checks_streamed_bytes_without_content_length(client, monkeypatch):
    http, app = client
    monkeypatch.setattr(settings, "request_max_bytes", 1024)

    async def body():
        yield b"x" * 800
        yield b"x" * 800

    response = await http.post("/v1/chat", content=body(), headers=HEADERS)
    assert response.status_code == 413
    app.state.pipeline.handle_chat.assert_not_called()


async def test_untrusted_input_bounds_are_rejected(client):
    http, _ = client
    for changed in ({"prompt": " "}, {"prompt": "x" * 64001}, {"max_tokens": 9000}, {"user_id": ""}):
        assert (await http.post("/v1/chat", json={**BODY, **changed}, headers=HEADERS)).status_code == 422


async def test_readiness_probes_dependencies_and_preserves_vector_degraded_mode(client):
    http, app = client
    assert (await http.get("/health/live")).status_code == 200
    assert (await http.get("/health/ready")).json()["status"] == "ready"
    app.state.graph.health.return_value = False
    response = await http.get("/health/ready")
    assert response.status_code == 200 and response.json()["status"] == "degraded"
    app.state.db.health.return_value = False
    assert (await http.get("/health/ready")).status_code == 503


async def test_metrics_use_separate_secret_and_bounded_route_labels(client):
    http, _ = client
    assert (await http.get("/metrics", headers=HEADERS)).status_code == 401
    await http.get("/unknown-private-user-555")
    response = await http.get("/metrics", headers={"Authorization": f"Bearer {METRICS_KEY}"})
    assert response.status_code == 200
    assert "gateway_http_requests_total" in response.text
    assert "unknown-private-user-555" not in response.text
    assert KEY not in response.text


async def test_chat_stops_accepting_work_before_resources_are_closed(client):
    http, app = client
    app.state.ready = False
    response = await http.post(
        "/v1/chat", content=json.dumps(BODY), headers={**HEADERS, "Content-Type": "application/json"}
    )
    assert response.status_code == 503
    app.state.pipeline.handle_chat.assert_not_called()


async def test_slow_uploads_are_bounded_without_blocking_liveness(client, monkeypatch):
    import asyncio

    http, _ = client
    monkeypatch.setattr(settings, "max_concurrent_requests", 1)
    monkeypatch.setattr(settings, "admission_timeout_seconds", 0.01)
    release, both_entered = asyncio.Event(), asyncio.Event()
    entered = 0

    async def slow_body():
        nonlocal entered
        entered += 1
        if entered == 2:
            both_entered.set()
        yield b"{"
        await release.wait()
        yield b"}"

    tasks = [
        asyncio.create_task(http.post("/v1/chat", content=slow_body(), headers=HEADERS)) for _ in range(2)
    ]
    try:
        await asyncio.wait_for(both_entered.wait(), timeout=1)
        response = await http.post("/v1/chat", json=BODY, headers=HEADERS)
        assert response.status_code == 503
        assert response.headers["retry-after"] == "1"
        assert (await http.get("/health/live")).status_code == 200
    finally:
        release.set()
        await asyncio.gather(*tasks)

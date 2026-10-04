"""Offline provider behavior: attempt budgets, bounded admission and circuit recovery."""
import asyncio
from types import SimpleNamespace

import anthropic
import httpx
import pytest

from app.config import settings
from app.providers import (
    AnthropicProvider,
    CircuitOpenError,
    ProviderClosedError,
    ProviderOverloadedError,
    Router,
    estimate_cost,
)


def config(**overrides):
    return SimpleNamespace(**{
        "anthropic_api_key": "unused", "simple_model": "claude-haiku-4-5",
        "complex_model": "claude-sonnet-4-5", "complex_prompt_chars": 600,
        "provider_max_concurrency": 2, "provider_timeout_seconds": 1,
        "provider_queue_timeout_seconds": 0.02, "provider_failure_threshold": 2,
        "provider_circuit_reset_seconds": 30, "provider_shutdown_timeout_seconds": 0.02,
        **overrides,
    })


def timeout_error():
    return anthropic.APITimeoutError(request=httpx.Request("POST", "https://test.invalid"))


def status_error(status):
    response = httpx.Response(status, request=httpx.Request("POST", "https://test.invalid"))
    return anthropic.APIStatusError("upstream rejected", response=response, body=None)


class FakeProvider:
    name = "anthropic"

    def __init__(self, failures=None, gate=None):
        self.models_called = []
        self.failures = failures or {}
        self.gate = gate
        self.started = asyncio.Event()
        self.closed = False
        self.active = 0
        self.max_active = 0

    async def complete(self, model, system, prompt, max_tokens):
        self.models_called.append(model)
        self.started.set()
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.gate is not None:
                await self.gate.wait()
            failures = self.failures.get(model, [])
            if failures:
                raise failures.pop(0)
            return "ok", 10, 5
        finally:
            self.active -= 1

    async def close(self):
        self.closed = True


@pytest.fixture
async def routers():
    created = []

    def create(provider=None, **overrides):
        router = Router(provider=provider or FakeProvider(), config=config(**overrides))
        created.append(router)
        return router

    yield create
    await asyncio.gather(*(router.close() for router in created))


def test_known_model_estimates_and_unknown_is_explicit():
    assert estimate_cost("claude-haiku-4-5", 1_000_000, 1_000_000) == pytest.approx(6.0)
    assert estimate_cost("claude-sonnet-4-5", 500_000, 100_000) == pytest.approx(3.0)
    assert estimate_cost("some-future-model", 1000, 1000) is None
    assert estimate_cost("claude-sonnet-5", 1000, 1000) is None


def test_configured_pricing_and_invalid_prices():
    assert estimate_cost("custom", 1000, 2000, pricing={"custom": (2, 4)}) == pytest.approx(0.01)
    with pytest.raises(ValueError):
        estimate_cost("custom", 1, 1, pricing={"custom": (-1, 4)})
    with pytest.raises(ValueError):
        estimate_cost("custom", 1, 1, pricing={"custom": (float("nan"), 4)})
    with pytest.raises(ValueError):
        estimate_cost("custom", -1, 1)


async def test_routing_preserves_prompt_complexity_policy(routers):
    router = routers()
    assert router.choose_model("What is a bake time?") == settings.simple_model
    assert router.choose_model("x" * 601) == "claude-sonnet-4-5"
    assert router.choose_model("Compare blue-green and canary deploys") == "claude-sonnet-4-5"


async def test_success_does_not_fallback(routers):
    router = routers()
    result = await router.complete("short", None, 100)
    assert result.text == "ok"
    assert result.model == "claude-haiku-4-5"
    assert result.fallback_used is False
    assert result.fallback_provider is None


@pytest.mark.parametrize("error", [timeout_error(), status_error(429), status_error(503)])
async def test_transient_failure_falls_back_exactly_once(routers, error):
    provider = FakeProvider(failures={"claude-haiku-4-5": [error]})
    router = routers(provider)
    result = await router.complete("short", None, 100)
    assert result.fallback_used is True
    assert result.fallback_provider == "anthropic"
    assert provider.models_called == ["claude-haiku-4-5", "claude-sonnet-4-5"]


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
async def test_nontransient_rejection_never_falls_back(routers, status):
    provider = FakeProvider(failures={"claude-haiku-4-5": [status_error(status)]})
    router = routers(provider)
    with pytest.raises(anthropic.APIStatusError):
        await router.complete("short", None, 100)
    assert provider.models_called == ["claude-haiku-4-5"]


async def test_both_tiers_failing_are_not_retried_recursively(routers):
    provider = FakeProvider(failures={
        "claude-haiku-4-5": [timeout_error()], "claude-sonnet-4-5": [timeout_error()],
    })
    with pytest.raises(anthropic.APITimeoutError):
        await routers(provider).complete("short", None, 100)
    assert len(provider.models_called) == 2


async def test_open_circuit_skips_primary_until_recovery_probe(routers):
    provider = FakeProvider(failures={"claude-haiku-4-5": [timeout_error(), timeout_error()]})
    router = routers(provider)
    await router.complete("short", None, 100)
    await router.complete("short", None, 100)
    result = await router.complete("short", None, 100)
    assert result.fallback_used
    assert provider.models_called.count("claude-haiku-4-5") == 2
    assert "claude-haiku-4-5" in router.health()["open_circuits"]
    router._circuits["claude-haiku-4-5"].open_until = 0.0001
    result = await router.complete("short", None, 100)
    assert not result.fallback_used
    assert router.health()["open_circuits"] == []


async def test_half_open_circuit_allows_only_one_probe(routers):
    provider = FakeProvider(failures={"claude-haiku-4-5": [timeout_error()]})
    router = routers(provider, provider_failure_threshold=1)
    await router.complete("short", None, 100)
    router._circuits["claude-haiku-4-5"].open_until = 0.0001
    provider.gate = asyncio.Event()
    provider.started.clear()
    probe = asyncio.create_task(router.complete("short", None, 100))
    await provider.started.wait()
    second = asyncio.create_task(router.complete("short", None, 100))
    await asyncio.sleep(0.005)
    assert provider.models_called.count("claude-haiku-4-5") == 2
    provider.gate.set()
    first_result, second_result = await asyncio.gather(probe, second)
    assert not first_result.fallback_used
    assert second_result.fallback_used


async def test_both_circuits_open_fail_without_provider_call(routers):
    provider = FakeProvider(failures={
        "claude-haiku-4-5": [timeout_error()], "claude-sonnet-4-5": [timeout_error()],
    })
    router = routers(provider, provider_failure_threshold=1)
    with pytest.raises(anthropic.APITimeoutError):
        await router.complete("short", None, 100)
    with pytest.raises(CircuitOpenError):
        await router.complete("short", None, 100)
    assert len(provider.models_called) == 2


async def test_queue_limit_and_queue_timeout_preserve_capacity(routers):
    provider = FakeProvider(gate=asyncio.Event())
    router = routers(provider, provider_max_concurrency=1)
    first = asyncio.create_task(router.complete("one", None, 100))
    await provider.started.wait()
    queued = asyncio.create_task(router.complete("two", None, 100))
    await asyncio.sleep(0)
    with pytest.raises(ProviderOverloadedError, match="full"):
        await router.complete("three", None, 100)
    with pytest.raises(ProviderOverloadedError, match="expired"):
        await queued
    assert provider.max_active == 1
    provider.gate.set()
    await first
    assert (await router.complete("four", None, 100)).text == "ok"
    assert router.health()["admitted_requests"] == 0


async def test_cancelled_queued_request_does_not_leak_slot(routers):
    provider = FakeProvider(gate=asyncio.Event())
    router = routers(provider, provider_max_concurrency=1)
    first = asyncio.create_task(router.complete("one", None, 100))
    await provider.started.wait()
    queued = asyncio.create_task(router.complete("two", None, 100))
    await asyncio.sleep(0)
    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    provider.gate.set()
    await first
    assert (await router.complete("three", None, 100)).text == "ok"


async def test_attempt_deadline_bounds_even_a_hung_provider(routers):
    provider = FakeProvider(gate=asyncio.Event())
    router = routers(provider, provider_timeout_seconds=0.01)
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(router.complete("short", None, 100), 0.1)
    assert len(provider.models_called) == 2
    assert provider.active == 0


async def test_shutdown_cancels_inflight_at_deadline_and_closes_sdk(routers):
    provider = FakeProvider(gate=asyncio.Event())
    router = routers(provider)
    request = asyncio.create_task(router.complete("short", None, 100))
    await provider.started.wait()
    await asyncio.wait_for(router.close(), 0.2)
    assert request.cancelled()
    assert provider.closed
    assert router.health()["admitted_requests"] == 0
    with pytest.raises(ProviderClosedError):
        await router.complete("later", None, 100)


async def test_sdk_has_explicit_timeout_and_zero_hidden_retries():
    provider = AnthropicProvider(config=config(provider_timeout_seconds=7))
    try:
        assert provider.client.max_retries == 0
        assert provider.client.timeout.read == 7
        assert provider.client.timeout.connect <= 5
    finally:
        await provider.close()

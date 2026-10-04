"""Atomic quota behavior, scope isolation, expiry and fail-closed Redis errors.

The optional integration test executes the actual Lua script on a configured Redis
service. It uses a random tenant prefix and deletes only that tenant's test keys.
"""
import asyncio
import hashlib
import os
import time
import uuid
from types import SimpleNamespace

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from app.config import settings
from app.rate_limit import WINDOW_SCRIPT, RateLimiter


class FakeRedis:
    """Atomic in-memory counter service; records the gateway's one-script protocol."""

    def __init__(self):
        self.values = {}
        self.expirations = {}
        self.calls = []
        self._lock = asyncio.Lock()
        self.error = None
        self.closed = False

    async def eval(self, script, key_count, *keys):
        if self.error is not None:
            raise self.error
        assert script == WINDOW_SCRIPT
        assert key_count == len(keys) == 2
        async with self._lock:
            self.calls.append((script, keys))
            counters = []
            for key in keys:
                count = self.values.get(key, 0) + 1
                self.values[key] = count
                if count == 1:
                    self.expirations[key] = 120
                counters.append(count)
            return counters

    async def ping(self):
        if self.error is not None:
            raise self.error
        return True

    async def aclose(self):
        self.closed = True


@pytest.fixture
def limiter(monkeypatch):
    monkeypatch.setattr(settings, "rate_limit_per_minute", 3)
    monkeypatch.setattr(settings, "tenant_rate_limit_per_minute", 10)
    monkeypatch.setattr("app.rate_limit.time", SimpleNamespace(time=lambda: 120.25))
    client = FakeRedis()
    limiter = RateLimiter()
    limiter.client = client
    return limiter


async def test_user_quota_is_atomic_under_concurrency(limiter):
    results = await asyncio.gather(*(limiter.check("reader", tenant_id="team-a") for _ in range(30)))
    assert sum(allowed for allowed, _ in results) == 3
    assert all(wait == 60 for allowed, wait in results if not allowed)
    assert len(limiter.client.calls) == 30  # One atomic script per decision, no multi-step races.
    assert sorted(limiter.client.values.values()) == [30, 30]
    assert set(limiter.client.expirations.values()) == {120}


async def test_tenant_quota_aggregates_distinct_users(limiter):
    results = await asyncio.gather(*(
        limiter.check(f"reader-{index}", tenant_id="team-a") for index in range(15)
    ))
    assert sum(allowed for allowed, _ in results) == 10
    # Requests rejected at the tenant boundary still consume an attempt in this fixed window.
    assert max(limiter.client.values.values()) == 15


async def test_same_user_in_another_tenant_has_independent_budget(limiter):
    for _ in range(3):
        assert await limiter.check("reader", tenant_id="team-a") == (True, 0)
    assert (await limiter.check("reader", tenant_id="team-a"))[0] is False
    assert await limiter.check("reader", tenant_id="team-b") == (True, 0)
    keys_a = limiter.client.calls[0][1]
    keys_b = limiter.client.calls[-1][1]
    assert set(keys_a).isdisjoint(keys_b)


async def test_keys_are_private_unambiguous_and_cluster_colocated(limiter):
    await limiter.check("secret-reader", tenant_id="secret-tenant")
    tenant_key, user_key = limiter.client.calls[-1][1]
    for key in (tenant_key, user_key):
        assert "secret-reader" not in key
        assert "secret-tenant" not in key
    assert tenant_key.split("{")[1].split("}")[0] == user_key.split("{")[1].split("}")[0]
    await limiter.check("b:c", tenant_id="a")
    first_pair = limiter.client.calls[-1][1]
    await limiter.check("c", tenant_id="a:b")
    assert set(first_pair).isdisjoint(limiter.client.calls[-1][1])


async def test_next_window_resets_quotas_without_extending_current_ttl(limiter, monkeypatch):
    for _ in range(3):
        await limiter.check("reader", tenant_id="team-a")
    old_keys = set(limiter.client.values)
    limiter.client.expirations = {key: 73 for key in old_keys}
    monkeypatch.setattr("app.rate_limit.time", SimpleNamespace(time=lambda: 179.9))
    assert await limiter.check("reader", tenant_id="team-a") == (False, 1)
    assert all(limiter.client.expirations[key] == 73 for key in old_keys)
    monkeypatch.setattr("app.rate_limit.time", SimpleNamespace(time=lambda: 180.0))
    assert await limiter.check("reader", tenant_id="team-a") == (True, 0)
    assert len(set(limiter.client.values) - old_keys) == 2


async def test_redis_failure_fails_closed_and_successful_reconnect_recovers(limiter):
    limiter.client.error = RedisConnectionError("service unavailable")
    with pytest.raises(RedisConnectionError):
        await limiter.check("reader", tenant_id="team-a")
    assert limiter.client.values == {}
    limiter.client.error = None
    assert await limiter.check("reader", tenant_id="team-a") == (True, 0)


async def test_connection_pool_and_socket_timeouts_are_bounded(monkeypatch):
    fake = FakeRedis()
    options = {}

    def connect(url, **kwargs):
        options.update(kwargs)
        return fake

    monkeypatch.setattr("app.rate_limit.redis.from_url", connect)
    limiter = RateLimiter()
    try:
        await limiter.connect()
        assert await limiter.health()
        assert options["socket_connect_timeout"] == settings.redis_timeout_seconds
        assert options["socket_timeout"] == settings.redis_timeout_seconds
        assert options["max_connections"] == settings.max_concurrent_requests + 8
    finally:
        await limiter.close()
    assert fake.closed


@pytest.mark.integration
@pytest.mark.skipif(os.getenv("RUN_SERVICE_TESTS") != "1", reason="RUN_SERVICE_TESTS=1 required")
async def test_real_redis_atomic_tenant_user_limits_expiry_and_isolation(monkeypatch):
    tenant = "ratelimit-integration-" + uuid.uuid4().hex
    other = tenant + "-other"
    # Freeze just the client's window to avoid a minute rollover changing the assertions.
    fixed_time = time.time()
    monkeypatch.setattr("app.rate_limit.time", SimpleNamespace(time=lambda: fixed_time))
    monkeypatch.setattr(settings, "rate_limit_per_minute", 3)
    monkeypatch.setattr(settings, "tenant_rate_limit_per_minute", 12)
    limiter = RateLimiter()
    await limiter.connect()
    try:
        results = await asyncio.gather(*(
            limiter.check("reader", tenant_id=tenant) for _ in range(6)
        ))
        assert sum(allowed for allowed, _ in results) == 3
        others = await asyncio.gather(*(
            limiter.check(f"reader-{index}", tenant_id=tenant) for index in range(10)
        ))
        assert sum(allowed for allowed, _ in others) == 6
        assert await limiter.check("reader", tenant_id=other) == (True, 0)
        tenant_hash = hashlib.sha256(tenant.encode()).hexdigest()
        keys = [key async for key in limiter.client.scan_iter(match=f"rl:{{{tenant_hash}}}:*")]
        assert len(keys) == 12  # Tenant + original reader + 10 other readers.
        ttl_values = await asyncio.gather(*(limiter.client.ttl(key) for key in keys))
        assert all(0 < ttl <= 120 for ttl in ttl_values)
        monkeypatch.setattr("app.rate_limit.time", SimpleNamespace(time=lambda: fixed_time + 60))
        assert await limiter.check("reader", tenant_id=tenant) == (True, 0)
    finally:
        for test_tenant in (tenant, other):
            tenant_hash = hashlib.sha256(test_tenant.encode()).hexdigest()
            keys = [key async for key in limiter.client.scan_iter(match=f"rl:{{{tenant_hash}}}:*")]
            if keys:
                await limiter.client.delete(*keys)
        await limiter.close()

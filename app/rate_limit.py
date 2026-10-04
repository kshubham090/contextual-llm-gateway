"""Atomic per-tenant and per-user fixed windows; Redis failure fails closed."""

import hashlib
import json
import time

import redis.asyncio as redis

from .config import settings

WINDOW_SCRIPT = """
local tenant = redis.call('INCR', KEYS[1])
if tenant == 1 then redis.call('EXPIRE', KEYS[1], 120) end
local user = redis.call('INCR', KEYS[2])
if user == 1 then redis.call('EXPIRE', KEYS[2], 120) end
return {tenant, user}
"""


class RateLimiter:
    def __init__(self) -> None:
        self.client: redis.Redis | None = None

    async def connect(self) -> None:
        self.client = redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=settings.redis_timeout_seconds,
            socket_timeout=settings.redis_timeout_seconds,
            max_connections=settings.max_concurrent_requests + 8,
        )
        await self.client.ping()

    async def close(self) -> None:
        if self.client:
            await self.client.aclose()

    async def health(self) -> bool:
        return bool(self.client and await self.client.ping())

    async def check(self, user_id: str, *, tenant_id: str) -> tuple[bool, int]:
        now = time.time()
        window = int(now // 60)
        tenant = hashlib.sha256(tenant_id.encode()).hexdigest()
        user = hashlib.sha256(json.dumps([tenant_id, user_id]).encode()).hexdigest()
        # A hash tag keeps both counters on the same Redis Cluster slot.
        tenant_key = f"rl:{{{tenant}}}:{window}:tenant"
        user_key = f"rl:{{{tenant}}}:{window}:{user}"
        tenant_count, user_count = await self.client.eval(WINDOW_SCRIPT, 2, tenant_key, user_key)
        allowed = (
            tenant_count <= settings.tenant_rate_limit_per_minute
            and user_count <= settings.rate_limit_per_minute
        )
        return allowed, 0 if allowed else max(1, 60 - int(now % 60))

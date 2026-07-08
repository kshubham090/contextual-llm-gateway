"""Redis fixed-window rate limiting, one window per user per minute."""
import time

import redis.asyncio as redis

from .config import settings


class RateLimiter:
    def __init__(self) -> None:
        self.client: redis.Redis | None = None

    async def connect(self) -> None:
        self.client = redis.from_url(settings.redis_url, decode_responses=True)
        await self.client.ping()

    async def close(self) -> None:
        if self.client:
            await self.client.aclose()

    async def check(self, user_id: str) -> tuple[bool, int]:
        """Returns (allowed, seconds_until_reset)."""
        window = int(time.time() // 60)
        key = f"rl:{user_id}:{window}"
        async with self.client.pipeline(transaction=True) as pipe:
            pipe.incr(key)
            pipe.expire(key, 60)
            count, _ = await pipe.execute()
        if count > settings.rate_limit_per_minute:
            return False, 60 - int(time.time() % 60)
        return True, 0

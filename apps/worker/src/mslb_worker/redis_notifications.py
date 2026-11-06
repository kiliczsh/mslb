"""Bounded disposable Redis hints with an unconditional PostgreSQL polling fallback."""

import asyncio
import re
from uuid import UUID

from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError

_PUBLISH = """
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('LTRIM', KEYS[1], 0, 127)
redis.call('EXPIRE', KEYS[1], 60)
return 1
"""


class RedisHints:
    def __init__(self, *, url: str, namespace: str) -> None:
        if not re.fullmatch(pattern=r"[a-z][a-z0-9_]{0,62}", string=namespace):
            raise ValueError("Invalid Redis hint namespace")
        self.key = f"mslb:wakeups:{namespace}"
        self.client = Redis.from_url(
            url=url,
            max_connections=2,
            socket_connect_timeout=0.1,
            socket_timeout=0.2,
            retry=Retry(backoff=NoBackoff(), retries=0),
            decode_responses=False,
        )

    async def publish(self, *, run_id: UUID) -> bool:
        try:
            async with asyncio.timeout(delay=0.25):
                # EVAL is a variadic Redis protocol call. One atomic command keeps
                # the cardinality and TTL bounds true even if the client dies.
                response: object = await self.client.eval(_PUBLISH, 1, self.key, str(run_id))
                return type(response) is int and response == 1
        except (RedisError, OSError, TimeoutError):
            return False

    async def wait(self) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 0.25
        try:
            async with asyncio.timeout(delay=0.25):
                value = await self.client.blpop(keys=[self.key], timeout=0.1)
                if value is not None:
                    return
        except (RedisError, OSError, TimeoutError):
            pass
        # Avoid a reconnect/error spin. Redis recovery, eviction or lost hints
        # cannot postpone the next unconditional DB claim beyond this interval.
        await asyncio.sleep(delay=max(0, deadline - loop.time()))

    async def close(self) -> None:
        await self.client.aclose()

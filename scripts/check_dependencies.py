"""Exercise actual Compose service connectivity without application mocks."""

import asyncio
import os

import asyncpg
from redis.asyncio import Redis


async def main() -> None:
    """Check SQL transactions and Redis through Compose DNS."""
    connection = await asyncpg.connect(dsn=os.environ["DATABASE_URL"], timeout=10)
    try:
        async with connection.transaction():
            result = await connection.fetchval("SELECT 1")
            if result != 1:
                raise RuntimeError("PostgreSQL query failed")
    finally:
        await connection.close()
    async with Redis.from_url(url=os.environ["REDIS_URL"], socket_timeout=10) as client:
        if not await client.ping():
            raise RuntimeError("Redis ping failed")
    print("PostgreSQL transaction and Redis PING passed")


if __name__ == "__main__":
    asyncio.run(main=main())

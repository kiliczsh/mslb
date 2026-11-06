"""Bounded PostgreSQL connections and repeatable-read transaction retry."""

import asyncio
import re
from collections.abc import Awaitable, Callable

from sqlalchemy import literal, select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from mslb_persistence.errors import StoreError as StoreError


class Database:
    """Own bounded PostgreSQL connections and repeatable-read transaction retries."""

    def __init__(self, url: str, schema: str = "public") -> None:
        if not re.fullmatch(pattern=r"[a-z][a-z0-9_]{0,62}", string=schema):
            raise StoreError(code="VALIDATION_ERROR", message="Invalid database schema")
        parsed = make_url(name_or_url=url)
        if parsed.get_backend_name() != "postgresql":
            raise StoreError(code="VALIDATION_ERROR", message="PostgreSQL is required")
        self.schema = schema
        self.engine: AsyncEngine = create_async_engine(
            url=parsed.set(drivername="postgresql+asyncpg"),
            pool_size=3,
            max_overflow=0,
            pool_timeout=5,
            pool_pre_ping=True,
            isolation_level="REPEATABLE READ",
            connect_args={
                "timeout": 5,
                "command_timeout": 15,
                "server_settings": {
                    "search_path": schema,
                    "statement_timeout": "15000",
                    "lock_timeout": "5000",
                    "idle_in_transaction_session_timeout": "30000",
                    "application_name": "mslb",
                },
            },
        )
        self.sessions = async_sessionmaker(bind=self.engine, expire_on_commit=False)

    async def transaction[T](self, operation: Callable[[AsyncSession], Awaitable[T]]) -> T:
        """Retry only serialization/deadlock conflicts, with a fresh bounded transaction."""
        for attempt in range(3):
            try:
                async with self.sessions.begin() as session:
                    return await operation(session)
            except DBAPIError as error:
                sqlstate = getattr(error.orig, "sqlstate", None)
                if sqlstate not in {"40001", "40P01"} or attempt == 2:
                    raise
                await asyncio.sleep(delay=0.01 * (attempt + 1))
        raise StoreError(code="INTERNAL_ERROR", message="Transaction retry exhausted")

    async def ready(self) -> bool:
        async with self.engine.connect() as connection:
            return await connection.scalar(statement=select(literal(value=1))) == 1

    async def close(self) -> None:
        await self.engine.dispose()

"""One PostgreSQL clock authority for leases, evidence freshness and final mutations."""

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from mslb_persistence.errors import StoreError


async def database_now(*, session: AsyncSession) -> datetime:
    """Read advancing server time, independent of the repeatable-read snapshot clock."""
    now = await session.scalar(statement=select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise StoreError(code="NOT_READY", message="Database clock unavailable")
    return now

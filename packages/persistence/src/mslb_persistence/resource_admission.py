"""Serialize workload growth against fresh fenced evidence in its owning transaction."""

from mslb_contracts.resources import ResourceDecision
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from mslb_persistence.database import Database
from mslb_persistence.errors import StoreError
from mslb_persistence.resource_evidence import decode
from mslb_persistence.resource_models import ResourceEvidence
from mslb_persistence.transaction_clock import database_now


async def decision(*, database: Database, session: AsyncSession) -> ResourceDecision | None:
    """Hold the publisher row until commit and re-evaluate its actual server-clock age."""
    if not database.enforce_resource_guard:
        return None
    row = await session.scalar(
        statement=select(ResourceEvidence).where(ResourceEvidence.id == 1).with_for_update()
    )
    if row is None:
        raise StoreError(code="NOT_READY", message="Resource evidence migration is missing")
    return decode(row=row, now=await database_now(session=session)).decision


async def admission_allowed(*, database: Database, session: AsyncSession) -> bool:
    """Permit new growth only with positive current evidence or an unguarded installation."""
    report = await decision(database=database, session=session)
    return report is None or report.admit


async def stop_required(*, database: Database, session: AsyncSession) -> bool:
    """Distinguish a runtime stop from the lower threshold that only holds new growth."""
    report = await decision(database=database, session=session)
    return report is not None and report.stop_running


async def require_no_stop(*, database: Database, session: AsyncSession) -> None:
    """Roll back final publication if current pressure or stale evidence requires stopping."""
    if await stop_required(database=database, session=session):
        raise StoreError(code="RESOURCE_PRESSURE", message="Resource evidence requires stopping")


async def require_admission(*, database: Database, session: AsyncSession) -> None:
    """Reject new materialization or ownership without releasing existing cleanup authority."""
    if not await admission_allowed(database=database, session=session):
        raise StoreError(
            code="RESOURCE_PRESSURE", message="Fresh safe resource evidence is required"
        )

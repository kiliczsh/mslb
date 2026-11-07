"""One fenced publisher and bounded latest resource evidence; no physical work here."""

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.resources import (
    ResourceDecision,
    ResourceProbe,
    host_memory_fresh,
    memory_bounds,
    safe_to_admit,
)
from pydantic import ValidationError
from sqlalchemy import DateTime, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from mslb_persistence.database import Database
from mslb_persistence.errors import StoreError
from mslb_persistence.resource_models import ResourceEvidence
from mslb_persistence.transaction_clock import database_now as _clock


@dataclass(frozen=True, slots=True, kw_only=True)
class EvidenceLease:
    """Carry the resource publisher's fencing token and optimistic revision."""

    owner: str
    fence: int
    expires_at: datetime
    revision: int


@dataclass(frozen=True, slots=True, kw_only=True)
class EvidenceRecord:
    """Return current verified measurements and an explicit admission or refusal decision."""

    revision: int
    received_at: datetime | None
    probe: ResourceProbe | None
    decision: ResourceDecision


def unavailable(*, revision: int, stale: bool = False) -> EvidenceRecord:
    """Create an explicit fail-closed result when resource evidence cannot be trusted."""
    return EvidenceRecord(
        revision=revision,
        received_at=None,
        probe=None,
        decision=ResourceDecision(
            admit=False,
            stop_running=True,
            reason="MEASUREMENTS_STALE" if stale else "MEASUREMENTS_UNAVAILABLE",
            cap_sum_bytes=0,
            required_headroom_bytes=2 * 1024**3,
        ),
    )


def validate_budget(
    *, probe: ResourceProbe | None, decision: ResourceDecision, code: str = "VALIDATION_ERROR"
) -> None:
    # Clock freshness is checked separately by the SQL transaction/reader. Here only
    # validate the independently derived capacity/headroom, allowing either sampler
    # to finish first. A future host frame still cannot pass the database-clock check.
    """Validate bounded resource evidence before persistence or admission reads."""
    budget_time = (
        datetime.now(tz=UTC)
        if probe is None
        else max(
            probe.measured_at,
            probe.measured_at if probe.host_memory is None else probe.host_memory.measured_at,
        )
    )
    if decision.admit and (probe is None or not safe_to_admit(probe=probe, now=budget_time)):
        raise StoreError(code=code, message="Admission requires actual safe evidence")
    caps, _reserve, headroom = memory_bounds(probe=probe)
    if (decision.cap_sum_bytes, decision.required_headroom_bytes) != (caps, headroom):
        raise StoreError(code=code, message="Evidence budget differs from actual counters")


def decode(*, row: ResourceEvidence, now: datetime) -> EvidenceRecord:
    """Reject stale/inconsistent stored frames with an explicit unavailable decision."""
    if row.decision is None or row.digest is None or row.received_at is None:
        return unavailable(revision=row.revision)
    try:
        probe = (
            None
            if row.probe is None
            else ResourceProbe.model_validate_json(json_data=canonical_bytes(value=row.probe))
        )
        decision = ResourceDecision.model_validate_json(
            json_data=canonical_bytes(value=row.decision)
        )
        validate_budget(probe=probe, decision=decision, code="NOT_READY")
        raw = canonical_bytes(value={"probe": row.probe, "decision": row.decision})
        if hashlib.sha256(string=raw).hexdigest() != row.digest:
            raise StoreError(code="NOT_READY", message="Resource evidence integrity differs")
    except ValidationError:
        raise StoreError(code="NOT_READY", message="Resource evidence is invalid") from None
    if (
        row.lease_expires_at is None
        or row.lease_expires_at <= now
        or not 0 <= (now - row.received_at).total_seconds() <= 3
        or (
            probe is not None
            and (
                not 0 <= (now - probe.measured_at).total_seconds() <= 3
                or (decision.admit and not host_memory_fresh(probe=probe, now=now))
            )
        )
    ):
        return unavailable(revision=row.revision, stale=True)
    return EvidenceRecord(
        revision=row.revision, received_at=row.received_at, probe=probe, decision=decision
    )


class ResourceEvidenceStore:
    """Fence resource publishers and expose only current, verified admission evidence."""

    def __init__(self, *, database: Database) -> None:
        self.database = database

    async def claim(self, *, owner: str) -> EvidenceLease | None:
        """Fence one resource publisher and invalidate the prior owner's report."""
        if not 1 <= len(owner.encode()) <= 128:
            raise StoreError(code="VALIDATION_ERROR", message="Invalid evidence publisher identity")

        async def operation(session: AsyncSession) -> EvidenceLease | None:
            row = await session.scalar(
                statement=select(ResourceEvidence)
                .where(ResourceEvidence.id == 1)
                .with_for_update(skip_locked=True)
            )
            if row is None:
                return None
            now = await _clock(session=session)
            if row.lease_expires_at is not None and row.lease_expires_at > now:
                return None
            row.owner, row.fence = owner, row.fence + 1
            expires = now + timedelta(seconds=10)
            row.lease_expires_at = expires
            row.probe, row.decision, row.digest, row.received_at = None, None, None, None
            row.revision += 1
            return EvidenceLease(
                owner=owner, fence=row.fence, expires_at=expires, revision=row.revision
            )

        return await self.database.transaction(operation=operation)

    async def publish(
        self,
        *,
        lease: EvidenceLease,
        expected_revision: int,
        probe: ResourceProbe | None,
        decision: ResourceDecision,
    ) -> EvidenceRecord:
        """Verify freshness/budget invariants and publish through an optimistic fence."""
        if type(expected_revision) is not int or expected_revision < 0:
            raise StoreError(code="VALIDATION_ERROR", message="Invalid resource evidence revision")
        decision = ResourceDecision.model_validate(obj=decision.model_dump())
        if probe is not None:
            probe = ResourceProbe.model_validate(obj=probe.model_dump())
        validate_budget(probe=probe, decision=decision)
        body: dict[str, object] = {
            "probe": None if probe is None else probe.model_dump(mode="json"),
            "decision": decision.model_dump(mode="json"),
        }
        raw = canonical_bytes(value=body)
        if len(raw) > 16 * 1024:
            raise StoreError(code="VALIDATION_ERROR", message="Evidence exceeds its bounded frame")

        async def operation(session: AsyncSession) -> EvidenceRecord:
            now = await _clock(session=session)
            if probe is not None and not 0 <= (now - probe.measured_at).total_seconds() <= 3:
                raise StoreError(
                    code="VALIDATION_ERROR", message="Evidence timestamp is not current server time"
                )
            if probe is not None and decision.admit and not host_memory_fresh(probe=probe, now=now):
                raise StoreError(
                    code="VALIDATION_ERROR", message="Admission requires current host evidence"
                )
            statement = (
                update(table=ResourceEvidence)
                .where(
                    ResourceEvidence.id == 1,
                    ResourceEvidence.owner == lease.owner,
                    ResourceEvidence.fence == lease.fence,
                    ResourceEvidence.revision == expected_revision,
                    ResourceEvidence.lease_expires_at > func.clock_timestamp(),
                )
                .values(
                    probe=body["probe"],
                    decision=body["decision"],
                    digest=hashlib.sha256(string=raw).hexdigest(),
                    received_at=func.clock_timestamp(),
                    lease_expires_at=func.clock_timestamp() + timedelta(seconds=10),
                    revision=ResourceEvidence.revision + 1,
                )
                .returning(ResourceEvidence)
            )
            if probe is not None:
                statement = statement.where(
                    or_(
                        ResourceEvidence.probe.is_(other=None),
                        ResourceEvidence.probe["measured_at"].astext.cast(
                            type_=DateTime(timezone=True)
                        )
                        <= probe.measured_at,
                    )
                )
            row = await session.scalar(statement=statement)
            if row is None:
                current = await session.get(entity=ResourceEvidence, ident=1)
                if (
                    current is not None
                    and current.owner == lease.owner
                    and current.fence == lease.fence
                    and current.lease_expires_at is not None
                    and current.lease_expires_at > await _clock(session=session)
                ):
                    raise StoreError(
                        code="CONFLICT", message="A newer resource evidence report exists"
                    )
                raise StoreError(
                    code="FENCE_LOST", message="Resource evidence publisher lost its lease"
                )
            return decode(row=row, now=await _clock(session=session))

        return await self.database.transaction(operation=operation)

    async def read(self) -> EvidenceRecord:
        """Read the latest validated resource frame with its persisted decision."""

        async def operation(session: AsyncSession) -> EvidenceRecord:
            row = await session.get(entity=ResourceEvidence, ident=1)
            if row is None:
                raise StoreError(code="NOT_READY", message="Resource evidence migration is missing")
            return decode(row=row, now=await _clock(session=session))

        return await self.database.transaction(operation=operation)

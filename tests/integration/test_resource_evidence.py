"""Real PostgreSQL fencing, freshness, payload integrity and admission-budget guards."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from mslb_application.resource_policy import ResourcePolicy
from mslb_contracts.resources import HostMemoryEvidence, ResourceDecision, ResourceProbe
from mslb_persistence import resource_evidence
from mslb_persistence.database import StoreError
from mslb_persistence.models import ResourceEvidence
from mslb_persistence.resource_evidence import ResourceEvidenceStore
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from tests.integration.helpers import scratch
from tests.test_resource_policy import probe as synthetic_probe


def healthy_evidence() -> tuple[ResourceProbe, ResourceDecision]:
    policy = ResourcePolicy()
    now = datetime.now(tz=UTC)
    probe = synthetic_probe().model_copy(update={"measured_at": now - timedelta(seconds=2)})
    decision = policy.evaluate(probe=probe, now=probe.measured_at)
    for offset in (1, 0):
        probe = synthetic_probe().model_copy(
            update={"measured_at": now - timedelta(seconds=offset)}
        )
        decision = policy.evaluate(probe=probe, now=probe.measured_at)
    return probe, decision


def test_one_fenced_publisher_records_actual_payload_then_fails_closed_on_age() -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)
            initial = await store.read()
            assert (
                not initial.decision.admit and initial.decision.reason == "MEASUREMENTS_UNAVAILABLE"
            )
            first, second = await asyncio.gather(
                store.claim(owner="first"), store.claim(owner="second")
            )
            leases = [lease for lease in (first, second) if lease is not None]
            assert len(leases) == 1
            lease = leases[0]
            probe, decision = healthy_evidence()
            assert decision.admit
            saved = await store.publish(
                lease=lease, expected_revision=lease.revision, probe=probe, decision=decision
            )
            assert saved.probe == probe and saved.decision.admit
            assert (await store.read()).revision == saved.revision
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=update(table=ResourceEvidence).values(
                        received_at=func.clock_timestamp() - timedelta(seconds=4)
                    )
                )
            stale = await store.read()
            assert stale.decision.reason == "MEASUREMENTS_STALE" and stale.decision.stop_running

    asyncio.run(main=run())


def test_expired_publisher_cannot_overwrite_new_owner_or_reuse_a_client_expiry() -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)
            old = await store.claim(owner="old")
            assert old is not None
            probe, decision = healthy_evidence()
            await store.publish(
                lease=old, expected_revision=old.revision, probe=probe, decision=decision
            )
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=update(table=ResourceEvidence).values(
                        lease_expires_at=func.clock_timestamp() - timedelta(seconds=1)
                    )
                )
            assert not (await store.read()).decision.admit
            replacement = await store.claim(owner="new")
            assert replacement is not None and replacement.fence > old.fence
            assert (await store.read()).decision.reason == "MEASUREMENTS_UNAVAILABLE"
            with pytest.raises(expected_exception=StoreError, match="lost its lease"):
                await store.publish(
                    lease=old, expected_revision=old.revision, probe=probe, decision=decision
                )
            assert not (await store.read()).decision.admit
            assert (
                await store.publish(
                    lease=replacement,
                    expected_revision=replacement.revision,
                    probe=probe,
                    decision=decision,
                )
            ).decision.admit

    asyncio.run(main=run())


def test_unsafe_budget_forged_admission_and_corrupted_payload_are_rejected() -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)
            lease = await store.claim(owner="guard")
            assert lease is not None
            probe, decision = healthy_evidence()
            with pytest.raises(expected_exception=StoreError, match="budget differs"):
                await store.publish(
                    lease=lease,
                    expected_revision=lease.revision,
                    probe=probe,
                    decision=decision.model_copy(update={"cap_sum_bytes": 1}),
                )
            pressure = probe.model_copy(update={"available_memory_bytes": 1})
            with pytest.raises(expected_exception=StoreError, match="safe evidence"):
                await store.publish(
                    lease=lease, expected_revision=lease.revision, probe=pressure, decision=decision
                )
            await store.publish(
                lease=lease, expected_revision=lease.revision, probe=probe, decision=decision
            )
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=update(table=ResourceEvidence).values(digest="f" * 64)
                )
            with pytest.raises(expected_exception=StoreError, match="integrity differs"):
                await store.read()

    asyncio.run(main=run())


def test_failure_frame_is_explicit_and_partial_sql_report_is_forbidden() -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)
            lease = await store.claim(owner="guard")
            assert lease is not None
            decision = ResourcePolicy().evaluate(probe=None, now=datetime.now(tz=UTC))
            saved = await store.publish(
                lease=lease, expected_revision=lease.revision, probe=None, decision=decision
            )
            assert saved.probe is None and saved.decision.reason == "MEASUREMENTS_UNAVAILABLE"
            with pytest.raises(expected_exception=IntegrityError):
                async with database.engine.begin() as connection:
                    await connection.execute(
                        statement=update(table=ResourceEvidence).values(digest=None)
                    )

    asyncio.run(main=run())


def test_same_publisher_cannot_overwrite_with_old_revision_or_older_measurement() -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)
            lease = await store.claim(owner="guard")
            assert lease is not None
            probe, decision = healthy_evidence()
            saved = await store.publish(
                lease=lease, expected_revision=lease.revision, probe=probe, decision=decision
            )
            failure = ResourcePolicy().evaluate(probe=None, now=datetime.now(tz=UTC))
            with pytest.raises(expected_exception=StoreError, match="newer resource evidence"):
                await store.publish(
                    lease=lease, expected_revision=lease.revision, probe=None, decision=failure
                )
            older = probe.model_copy(
                update={"measured_at": probe.measured_at - timedelta(seconds=0.1)}
            )
            with pytest.raises(expected_exception=StoreError, match="newer resource evidence"):
                await store.publish(
                    lease=lease, expected_revision=saved.revision, probe=older, decision=decision
                )
            assert (await store.read()).revision == saved.revision
            assert (await store.read()).decision.admit

    asyncio.run(main=run())


def test_real_database_expiry_during_publication_rolls_back_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)
            lease = await store.claim(owner="guard")
            assert lease is not None
            probe, decision = healthy_evidence()
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=update(table=ResourceEvidence).values(
                        lease_expires_at=func.clock_timestamp() + timedelta(seconds=0.03)
                    )
                )
            original = resource_evidence._clock

            async def slow_clock(*, session: AsyncSession) -> datetime:
                now = await original(session=session)
                await asyncio.sleep(delay=0.06)
                return now

            monkeypatch.setattr(target=resource_evidence, name="_clock", value=slow_clock)
            with pytest.raises(expected_exception=StoreError, match="lost its lease"):
                await store.publish(
                    lease=lease, expected_revision=lease.revision, probe=probe, decision=decision
                )
            async with database.sessions() as session:
                row = await session.scalar(
                    statement=select(ResourceEvidence).where(ResourceEvidence.id == 1)
                )
                assert row is not None and row.decision is None and row.received_at is None

    asyncio.run(main=run())


def test_desktop_host_evidence_is_rechecked_with_database_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)
            lease = await store.claim(owner="desktop-guard")
            assert lease is not None
            policy = ResourcePolicy()
            now = datetime.now(tz=UTC)
            for offset in (2, 1, 0):
                current = now - timedelta(seconds=offset)
                probe = synthetic_probe().model_copy(
                    update={
                        "measured_at": current,
                        "desktop_engine": True,
                        "host_memory": HostMemoryEvidence(
                            measured_at=current - timedelta(seconds=2),
                            total_memory_bytes=32 * 1024**3,
                            available_memory_bytes=10 * 1024**3,
                        ),
                    }
                )
                decision = policy.evaluate(probe=probe, now=current)
            assert decision.admit
            saved = await store.publish(
                lease=lease, expected_revision=lease.revision, probe=probe, decision=decision
            )
            assert saved.decision.admit
            original = resource_evidence._clock

            async def later_clock(*, session: AsyncSession) -> datetime:
                return await original(session=session) + timedelta(seconds=2)

            monkeypatch.setattr(target=resource_evidence, name="_clock", value=later_clock)
            # VM evidence remains <3s old while independent host evidence is >3s old.
            assert (await store.read()).decision.reason == "MEASUREMENTS_STALE"
            with pytest.raises(expected_exception=StoreError, match="current host evidence"):
                await store.publish(
                    lease=lease,
                    expected_revision=saved.revision,
                    probe=probe,
                    decision=decision,
                )

    asyncio.run(main=run())

"""Real PostgreSQL fencing, freshness, payload integrity and admission-budget guards."""

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta

import pytest
from mslb_application.resource_policy import ResourcePolicy
from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.resources import ResourceDecision, ResourceProbe
from mslb_persistence import resource_evidence
from mslb_persistence.database import StoreError
from mslb_persistence.models import ResourceEvidence
from mslb_persistence.resource_evidence import ResourceEvidenceStore
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from tests.integration.helpers import scratch
from tests.unit.platform.test_resource_policy import probe as synthetic_probe


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


@pytest.mark.parametrize(argnames="seconds", argvalues=(-1, 4))
def test_engine_evidence_is_rechecked_with_database_clock(
    monkeypatch: pytest.MonkeyPatch, seconds: int
) -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)
            lease = await store.claim(owner="desktop-guard")
            assert lease is not None
            probe, decision = healthy_evidence()
            probe = probe.model_copy(update={"desktop_engine": True})
            saved = await store.publish(
                lease=lease, expected_revision=lease.revision, probe=probe, decision=decision
            )

            async def shifted_clock(*, session: AsyncSession) -> datetime:
                return probe.measured_at + timedelta(seconds=seconds)

            with monkeypatch.context() as patch:
                patch.setattr(target=resource_evidence, name="_clock", value=shifted_clock)
                assert (await store.read()).decision.reason == "MEASUREMENTS_STALE"
                with pytest.raises(
                    expected_exception=resource_evidence.EvidenceRejected,
                    match="timestamp is not current",
                ):
                    await store.publish(
                        lease=lease,
                        expected_revision=saved.revision,
                        probe=probe,
                        decision=decision,
                    )
            # A pre-write rejection preserves the acknowledged revision and lease.
            current, decision = healthy_evidence()
            recovered = await store.publish(
                lease=lease, expected_revision=saved.revision, probe=current, decision=decision
            )
            assert recovered.decision.admit and recovered.revision == saved.revision + 1

    asyncio.run(main=run())


@pytest.mark.parametrize(argnames="desktop", argvalues=(False, True))
def test_legacy_evidence_is_rejected_until_current_publisher_replaces_it(desktop: bool) -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)
            lease = await store.claim(owner="guard")
            assert lease is not None
            probe, decision = healthy_evidence()
            saved = await store.publish(
                lease=lease, expected_revision=lease.revision, probe=probe, decision=decision
            )
            legacy = probe.model_dump(mode="json") | {
                "schema_version": 1,
                "desktop_engine": desktop,
            }
            legacy["host_memory"] = None
            if desktop:
                legacy["host_memory"] = {
                    "schema_version": 1,
                    "measured_at": probe.measured_at.isoformat(),
                    "total_memory_bytes": 32 * 1024**3,
                    "available_memory_bytes": 10 * 1024**3,
                }
            body = {"probe": legacy, "decision": decision.model_dump(mode="json")}
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=update(table=ResourceEvidence).values(
                        probe=legacy,
                        digest=hashlib.sha256(string=canonical_bytes(value=body)).hexdigest(),
                    )
                )
            with pytest.raises(expected_exception=StoreError, match="evidence is invalid"):
                await store.read()
            # Latest evidence is ephemeral; replacement needs no data/volume reset.
            current, decision = healthy_evidence()
            await store.publish(
                lease=lease, expected_revision=saved.revision, probe=current, decision=decision
            )
            assert (await store.read()).decision.admit

    asyncio.run(main=run())

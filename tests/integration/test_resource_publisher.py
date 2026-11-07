"""The real PostgreSQL publisher loop preserves ownership, recovery and shutdown safety."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from mslb_application.resource_guard import ResourcePublisher
from mslb_contracts.resources import ResourceDecision, ResourceProbe
from mslb_persistence.database import StoreError
from mslb_persistence.models import ResourceEvidence
from mslb_persistence.resource_evidence import EvidenceLease, EvidenceRecord, ResourceEvidenceStore
from sqlalchemy import func, update

from tests.integration.helpers import scratch
from tests.unit.platform.test_resource_policy import probe as synthetic_probe


def fresh() -> ResourceProbe:
    return synthetic_probe().model_copy(update={"measured_at": datetime.now(tz=UTC)})


def delay_first_sample(*, store: ResourceEvidenceStore, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the real SQL timestamp check reject one sample without a timing-dependent sleep."""
    original_publish = store.publish
    delayed = False

    async def publish(
        *,
        lease: EvidenceLease,
        expected_revision: int,
        probe: ResourceProbe | None,
        decision: ResourceDecision,
    ) -> EvidenceRecord:
        nonlocal delayed
        if not delayed:
            assert probe is not None
            delayed = True
            probe = probe.model_copy(
                update={"measured_at": datetime.now(tz=UTC) - timedelta(seconds=4)}
            )
        return await original_publish(
            lease=lease, expected_revision=expected_revision, probe=probe, decision=decision
        )

    monkeypatch.setattr(target=store, name="publish", value=publish)


def test_rejected_sample_preserves_acknowledged_publisher_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)
            delay_first_sample(store=store, monkeypatch=monkeypatch)

            async def measure() -> ResourceProbe:
                return fresh()

            publisher = ResourcePublisher(store=store, owner="rejected-sample", measure=measure)
            with pytest.raises(expected_exception=StoreError, match="timestamp"):
                await publisher.publish_once()
            assert publisher.lease is not None
            lease, revision = publisher.lease, publisher.revision
            assert publisher.policy.held and publisher.policy.cool_samples == 0

            report = await publisher.publish_once()
            assert report is not None and report.revision == revision + 1
            assert publisher.lease == lease
            assert report.decision.reason == "WARMING_UP" and not report.decision.admit
            async with database.sessions() as session:
                row = await session.get(entity=ResourceEvidence, ident=1)
                assert row is not None and (row.owner, row.fence) == (lease.owner, lease.fence)

    asyncio.run(main=run())


@pytest.mark.parametrize(argnames="fault", argvalues=["sample_rejection", "heartbeat"])
def test_publisher_loop_retries_without_waiting_for_its_own_lease(
    monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)
            if fault == "sample_rejection":
                delay_first_sample(store=store, monkeypatch=monkeypatch)
            stopped = asyncio.Event()
            reports: list[EvidenceRecord] = []
            fences: list[int] = []
            measurements = 0

            async def measure() -> ResourceProbe:
                nonlocal measurements
                measurements += 1
                return fresh()

            publisher = ResourcePublisher(store=store, owner="loop-retry", measure=measure)

            async def published(report: EvidenceRecord) -> None:
                assert publisher.lease is not None
                reports.append(report)
                fences.append(publisher.lease.fence)
                if fault == "heartbeat" and len(reports) == 1:
                    raise OSError("Injected heartbeat write failure after acknowledged publication")
                stopped.set()

            task = asyncio.create_task(coro=publisher.run(stopped=stopped, published=published))
            try:
                await asyncio.wait_for(fut=asyncio.shield(arg=task), timeout=4)
            finally:
                stopped.set()
                await task
            assert measurements == 2
            assert len(reports) == (1 if fault == "sample_rejection" else 2)
            assert len(set(fences)) == 1
            assert reports[-1].decision.reason == "WARMING_UP"
            assert not reports[-1].decision.admit

    asyncio.run(main=run())


def test_unknown_publish_receipt_still_discards_local_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)
            original_publish = store.publish

            async def lose_receipt(
                *,
                lease: EvidenceLease,
                expected_revision: int,
                probe: ResourceProbe | None,
                decision: ResourceDecision,
            ) -> EvidenceRecord:
                await original_publish(
                    lease=lease, expected_revision=expected_revision, probe=probe, decision=decision
                )
                raise TimeoutError("Injected lost commit receipt")

            async def measure() -> ResourceProbe:
                return fresh()

            monkeypatch.setattr(target=store, name="publish", value=lose_receipt)
            publisher = ResourcePublisher(store=store, owner="uncertain-receipt", measure=measure)
            with pytest.raises(expected_exception=TimeoutError):
                await publisher.publish_once()
            committed = await store.read()
            assert committed.probe is not None
            assert publisher.lease is None and publisher.revision == 0
            assert publisher.policy.held and publisher.policy.cool_samples == 0
            assert await publisher.publish_once() is None

    asyncio.run(main=run())


def test_publisher_warms_recovers_after_missing_measurements_and_rejects_followers() -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)
            missing = False
            calls = 0

            async def measure() -> ResourceProbe | None:
                nonlocal calls
                calls += 1
                return None if missing else fresh()

            publisher = ResourcePublisher(store=store, owner="leader", measure=measure)
            follower = ResourcePublisher(store=store, owner="follower", measure=measure)
            for admitted in (False, False, True):
                report = await publisher.publish_once()
                assert report is not None and report.decision.admit is admitted
            assert await follower.publish_once() is None and calls == 3
            missing = True
            report = await publisher.publish_once()
            assert report is not None and report.decision.stop_running and report.probe is None
            missing = False
            for admitted in (False, False, True):
                report = await publisher.publish_once()
                assert report is not None and report.decision.admit is admitted
            await publisher.invalidate()
            assert (await store.read()).decision.reason == "MEASUREMENTS_UNAVAILABLE"

    asyncio.run(main=run())


def test_replaced_publisher_resets_after_fence_loss_and_cannot_overwrite_recovery() -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)

            async def measure() -> ResourceProbe:
                return fresh()

            old = ResourcePublisher(store=store, owner="old", measure=measure)
            await old.publish_once()
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=update(table=ResourceEvidence).values(
                        lease_expires_at=func.clock_timestamp() - timedelta(seconds=1)
                    )
                )
            new = ResourcePublisher(store=store, owner="new", measure=measure)
            first = await new.publish_once()
            assert first is not None and first.decision.reason == "WARMING_UP"
            with pytest.raises(expected_exception=StoreError, match="lost its lease"):
                await old.publish_once()
            assert old.lease is None and old.policy.held and old.policy.cool_samples == 0
            assert (await store.read()).revision == first.revision
            assert await old.publish_once() is None

    asyncio.run(main=run())


def test_background_loop_has_no_overlapping_samples_and_invalidates_on_shutdown() -> None:
    async def run() -> None:
        async with scratch() as database:
            store = ResourceEvidenceStore(database=database)
            stopped, entered, released = asyncio.Event(), asyncio.Event(), asyncio.Event()
            calls = 0

            async def measure() -> ResourceProbe:
                nonlocal calls
                calls += 1
                if calls == 1:
                    entered.set()
                    await released.wait()
                else:
                    stopped.set()
                return fresh()

            publisher = ResourcePublisher(store=store, owner="loop", measure=measure)
            task = asyncio.create_task(coro=publisher.run(stopped=stopped))
            try:
                await asyncio.wait_for(fut=entered.wait(), timeout=2)
                with pytest.raises(expected_exception=StoreError, match="occupied"):
                    await publisher.publish_once()
                released.set()
                await asyncio.wait_for(fut=task, timeout=4)
                assert calls == 2 and publisher.lease is None
                assert (await store.read()).decision.reason == "MEASUREMENTS_UNAVAILABLE"
            finally:
                stopped.set()
                released.set()
                await task

    asyncio.run(main=run())

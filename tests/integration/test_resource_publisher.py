"""The real PostgreSQL publisher loop preserves ownership, recovery and shutdown safety."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from mslb_application.resource_guard import ResourcePublisher
from mslb_contracts.resources import ResourceProbe
from mslb_persistence.database import StoreError
from mslb_persistence.models import ResourceEvidence
from mslb_persistence.resource_evidence import ResourceEvidenceStore
from sqlalchemy import func, update

from tests.integration.helpers import scratch
from tests.test_resource_policy import probe as synthetic_probe


def fresh() -> ResourceProbe:
    return synthetic_probe().model_copy(update={"measured_at": datetime.now(tz=UTC)})


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
                    statement=update(ResourceEvidence).values(
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

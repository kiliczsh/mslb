"""Actual SQL freshness, resource binding and stale-owner exclusion for live frames."""

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from mslb_contracts.docker_artifacts import DockerProgress
from mslb_contracts.docker_observations import BackendSample
from mslb_persistence.database import StoreError
from mslb_persistence.docker_models import DockerRunState
from mslb_persistence.docker_progress import DockerProgressStore
from mslb_persistence.docker_reads import DockerReads
from sqlalchemy import update

from tests.integration.helpers import scratch
from tests.integration.test_docker_control import claim, expire, prepared
from tests.test_runtime_observations import health


def test_live_frames_require_current_scope_ownership_and_independent_freshness() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            run = await data.enqueue()
            lease = await claim(data=data)
            resource = await data.resources.reserve_static(lease=lease, name="backend-a")
            assert resource.container_id is not None
            observed = datetime.now(tz=UTC)
            frame = DockerProgress(
                run_id=run.id,
                fence=lease.fence,
                snapshot_digest=data.snapshot.digest(),
                observed_at=observed,
                elapsed_seconds=1.0,
                pending=1,
                inflight=1,
                backends=(
                    BackendSample(
                        resource_id=resource.id,
                        container_id=resource.container_id,
                        observed_at=observed - timedelta(milliseconds=1),
                        health=health(),
                    ),
                ),
                unavailable_backends=(),
            )
            store = DockerProgressStore(database=database)
            reads = DockerReads(database=database)
            await store.write(lease=lease, frame=frame)
            assert (await reads.run(run_id=run.id)).progress == frame
            with pytest.raises(expected_exception=StoreError):
                await store.write(
                    lease=lease, frame=frame.model_copy(update={"fence": lease.fence + 1})
                )
            stale = frame.model_copy(
                update={
                    "observed_at": frame.observed_at - timedelta(seconds=4),
                    "backends": tuple(
                        sample.model_copy(
                            update={"observed_at": sample.observed_at - timedelta(seconds=4)}
                        )
                        for sample in frame.backends
                    ),
                }
            )
            with pytest.raises(expected_exception=StoreError):
                await store.write(lease=lease, frame=stale)
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=update(table=DockerRunState)
                    .where(DockerRunState.run_id == run.id)
                    .values(latest_progress=stale.model_dump(mode="json"))
                )
            assert (await reads.run(run_id=run.id)).progress is None
            await expire(database=database)
            assert await data.control.claim(owner="recover") is not None
            with pytest.raises(expected_exception=StoreError) as rejected:
                await store.write(lease=lease, frame=frame)
            assert rejected.value.code == "FENCE_LOST"
            assert (await reads.run(run_id=run.id)).progress is None

    asyncio.run(main=scenario())

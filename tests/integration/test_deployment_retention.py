"""Enrollment retention never removes run references or authorizes daemon cleanup."""

import asyncio

import pytest
from mslb_contracts.docker_runs import DeploymentSpec, StaticBackend
from mslb_persistence.database import StoreError
from mslb_persistence.docker_catalog import DockerCatalog
from mslb_persistence.models import DockerDeployment
from sqlalchemy import func, select

from tests.integration.helpers import scratch
from tests.integration.test_docker_control import prepared


def definition(*, index: int) -> DeploymentSpec:
    return DeploymentSpec(
        image_id="sha256:" + f"{index:064x}",
        network_id="d" * 64,
        static=(
            StaticBackend(name="backend-a", container_id="a" * 64),
            StaticBackend(name="backend-b", container_id="b" * 64),
        ),
    )


def test_enrollment_reclaims_only_unused_old_history_at_its_bound() -> None:
    async def run() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            registered = await data.enqueue(key="retained-history")
            catalog = data.catalog
            history = []
            for index in range(1, 16):
                history.append(await catalog.enroll(definition=definition(index=index)))
            newest = await catalog.enroll(definition=definition(index=16))
            assert (
                await catalog.snapshot(run_id=registered.id)
            ).deployment_id == data.snapshot.deployment_id
            assert (await catalog.deployment(deployment_id=history[-1].id)).id == history[-1].id
            assert (await catalog.deployment(deployment_id=history[-2].id)).id == history[-2].id
            assert (await catalog.deployment(deployment_id=newest.id)).id == newest.id
            with pytest.raises(expected_exception=StoreError, match="does not exist"):
                await catalog.deployment(deployment_id=history[0].id)
            async with database.sessions() as session:
                assert (
                    await session.scalar(
                        statement=select(func.count()).select_from(DockerDeployment)
                    )
                    == 4
                )
            assert await catalog.prune_deployments(keep_recent=1, limit=1) == 1
            assert (
                await catalog.snapshot(run_id=registered.id)
            ).deployment_id == data.snapshot.deployment_id

    asyncio.run(main=run())


def test_retention_and_enrollment_races_keep_a_bounded_consistent_catalog() -> None:
    async def run() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            await data.enqueue(key="protected")
            for index in range(1, 12):
                await data.catalog.enroll(definition=definition(index=index))
            enrolled, _deleted = await asyncio.gather(
                data.catalog.enroll(definition=definition(index=12)),
                data.catalog.prune_deployments(keep_recent=2, limit=4),
            )
            assert (await data.catalog.deployment(deployment_id=enrolled.id)).id == enrolled.id
            assert (
                await data.catalog.deployment(deployment_id=data.snapshot.deployment_id)
            ).id == data.snapshot.deployment_id

    asyncio.run(main=run())


def test_invalid_retention_bounds_are_rejected_before_database_access() -> None:
    async def run() -> None:
        async with scratch() as database:
            catalog = DockerCatalog(database=database)
            for bounds in ({"keep_recent": 0}, {"limit": 17}, {"limit": True}):
                with pytest.raises(expected_exception=StoreError, match="retention bounds"):
                    await catalog.prune_deployments(**bounds)

    asyncio.run(main=run())


def test_full_referenced_history_refuses_enrollment_without_deleting_any_run_input() -> None:
    async def run() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            runs = [await data.enqueue(key="first")]
            for index in range(1, 16):
                deployment = await data.catalog.enroll(definition=definition(index=index))
                snapshot = data.snapshot.model_copy(
                    update={
                        "deployment_id": deployment.id,
                        "deployment_digest": deployment.definition.digest(),
                    }
                )
                runs.append(await data.catalog.enqueue(snapshot=snapshot))
            with pytest.raises(expected_exception=StoreError, match="history requires"):
                await data.catalog.enroll(definition=definition(index=16))
            assert await data.catalog.prune_deployments(keep_recent=1, limit=16) == 0
            for run in runs:
                assert (await data.catalog.snapshot(run_id=run.id)).deployment_id is not None
            async with database.sessions() as session:
                assert (
                    await session.scalar(
                        statement=select(func.count()).select_from(DockerDeployment)
                    )
                    == 16
                )

    asyncio.run(main=run())

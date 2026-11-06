"""Real PostgreSQL Docker registration after verified physical snapshot publication."""

import asyncio
import io
from pathlib import Path
from uuid import uuid4

import pytest
from mslb_application.docker_inputs import DockerSubmissions, load_docker_snapshot
from mslb_application.errors import OperationError
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_contracts.docker_runs import DeploymentSpec, DockerSettings, StaticBackend
from mslb_persistence.catalog import Catalog
from mslb_persistence.database import StoreError
from mslb_persistence.docker_catalog import DockerCatalog
from mslb_persistence.docker_control import DockerControl
from mslb_persistence.jobs import Jobs
from mslb_persistence.models import Job
from sqlalchemy import func, select

from tests.integration.helpers import scratch


def definition() -> DeploymentSpec:
    # Only the proxy may establish these identities in the installed service. This test
    # exercises file/database ordering with synthetic enrollment, not daemon inspection.
    return DeploymentSpec(
        image_id="sha256:" + "a" * 64,
        network_id="b" * 64,
        static=(
            StaticBackend(name="backend-a", container_id="c" * 64),
            StaticBackend(name="backend-b", container_id="d" * 64),
        ),
    )


def test_docker_job_has_verified_snapshot_before_claim_and_idempotent_retry(tmp_path: Path) -> None:
    async def run() -> None:
        async with scratch() as database:
            catalog, docker = Catalog(database=database), DockerCatalog(database=database)
            files = ManagedFiles(root=tmp_path, limits=StorageLimits(min_free_bytes=0))
            registered = files.ingest(
                source=io.BytesIO(
                    initial_bytes=b"t,request_id,work_units,mem_mb\n0,first,1,1\n1,second,2,1\n"
                ),
                filename="requests.csv",
            )
            dataset = await catalog.register_dataset(data=registered)
            enrolled = await docker.enroll(definition=definition())
            service = DockerSubmissions(catalog=catalog, docker=docker, files=files)
            settings = DockerSettings(duration_seconds=2)
            run = await service.submit(
                dataset_id=dataset.id, settings=settings, deployment_id=enrolled.id, key="same"
            )
            saved = load_docker_snapshot(
                files=files, key=run.snapshot_key, expected_digest=run.snapshot_digest
            )
            assert saved == await docker.snapshot(run_id=run.id)
            assert saved.dataset_id == dataset.id and saved.deployment_id == enrolled.id
            assert await Jobs(database=database).claim(owner="logical") is None
            lease = await DockerControl(database=database).claim(owner="controller")
            assert lease is not None and lease.run_id == run.id
            # An existing receipt must not need the latest deployment or rematerialize files.
            again = await service.submit(
                dataset_id=dataset.id, settings=settings, deployment_id=uuid4(), key="same"
            )
            assert again.id == run.id and again.snapshot_key == run.snapshot_key
            with pytest.raises(expected_exception=StoreError, match="different request"):
                await service.submit(
                    dataset_id=dataset.id,
                    settings=settings.model_copy(update={"duration_seconds": 3}),
                    deployment_id=enrolled.id,
                    key="same",
                )

    asyncio.run(main=run())


def test_corrupt_input_and_snapshot_failure_never_register_a_docker_job(tmp_path: Path) -> None:
    async def run() -> None:
        async with scratch() as database:
            catalog, docker = Catalog(database=database), DockerCatalog(database=database)
            files = ManagedFiles(root=tmp_path, limits=StorageLimits(min_free_bytes=0))
            registered = files.ingest(
                source=io.BytesIO(initial_bytes=b"t,request_id,work_units,mem_mb\n0,r,1,1\n"),
                filename="requests.csv",
            )
            dataset = await catalog.register_dataset(data=registered)
            enrolled = await docker.enroll(definition=definition())
            service = DockerSubmissions(catalog=catalog, docker=docker, files=files)
            path = files.path(key=dataset.storage_key)
            original = path.read_bytes()
            path.write_bytes(data=b"wrong")
            with pytest.raises(expected_exception=OperationError, match="length mismatch"):
                await service.submit(
                    dataset_id=dataset.id, settings=DockerSettings(), deployment_id=enrolled.id
                )
            path.write_bytes(data=original)
            files.limits = StorageLimits(quota_bytes=1, min_free_bytes=0)
            with pytest.raises(expected_exception=OperationError, match="quota"):
                await service.submit(
                    dataset_id=dataset.id, settings=DockerSettings(), deployment_id=enrolled.id
                )
            async with database.sessions() as session:
                assert await session.scalar(statement=select(func.count()).select_from(Job)) == 0

    asyncio.run(main=run())

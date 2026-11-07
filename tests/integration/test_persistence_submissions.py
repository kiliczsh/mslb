"""Real PostgreSQL submissions behavior in disposable schemas."""

import asyncio
import hashlib
import io
from pathlib import Path
from uuid import uuid4

import pytest
from mslb_application.artifacts import execute_bundle
from mslb_application.files import FileReference, ManagedFiles, StorageLimits
from mslb_application.submissions import Submissions
from mslb_contracts.catalog import DatasetRegistration
from mslb_contracts.inputs import InputError
from mslb_contracts.runs import RunSettings
from mslb_persistence.database import StoreError
from mslb_persistence.models import (
    Dataset,
    Job,
    Outbox,
    Run,
)
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from tests.integration.helpers import scratch
from tests.integration.persistence_helpers import prepare


def test_snapshot_integrity_and_dataset_metadata_fail_before_job_creation() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            with pytest.raises(expected_exception=InputError):
                await prepared.catalog.enqueue(
                    dataset_id=prepared.dataset.id,
                    snapshot=prepared.snapshot.model_copy(update={"snapshot_digest": "0" * 64}),
                    versions=prepared.versions,
                    body_digest=prepared.snapshot.snapshot_digest,
                )
            with pytest.raises(expected_exception=StoreError):
                await prepared.catalog.register_dataset(
                    data=DatasetRegistration(
                        digest=prepared.dataset.digest,
                        source_filename="other.csv",
                        request_count=1,
                        earliest_arrival=0,
                        latest_arrival=0,
                        size_bytes=1,
                    )
                )
            async with database.sessions() as session:
                assert await session.scalar(statement=select(func.count()).select_from(Run)) == 0
            with pytest.raises(expected_exception=StoreError):
                await prepared.catalog.get_run(run_id=uuid4())

    asyncio.run(main=scenario())


def test_submission_persists_verified_inputs_before_job_and_frozen_run_is_executable(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            files = await asyncio.to_thread(
                ManagedFiles, root=tmp_path, limits=StorageLimits(min_free_bytes=0)
            )
            service = Submissions(catalog=prepared.catalog, files=files)
            source = b"request_id,t,work_units,mem_mb,extra\r\nr2,1,3,2,x\r\nr1,0,4,1,y\r\n"
            dataset = await service.upload(
                source=io.BytesIO(initial_bytes=source), filename="../../source.csv"
            )
            assert dataset.original_digest == hashlib.sha256(data=source).hexdigest()
            assert dataset.source_size_bytes == len(source)
            settings = RunSettings(strategy="round_robin")
            run = await service.submit(
                dataset_id=dataset.id, settings=settings, key="durable-submission"
            )
            saved = await asyncio.to_thread(
                files.load_snapshot, key=run.snapshot_key, expected_digest=run.snapshot_digest
            )
            assert (await prepared.jobs.get(run_id=run.id)).status == "queued"
            for server in await prepared.catalog.list_servers():
                await prepared.catalog.delete_server(
                    server_id=server.id, expected_version=server.version
                )
            assert (
                await service.submit(
                    dataset_id=dataset.id, settings=settings, key="durable-submission"
                )
                == run
            )
            with pytest.raises(expected_exception=StoreError, match="different request"):
                await service.submit(
                    dataset_id=dataset.id, settings=RunSettings(), key="durable-submission"
                )
            manifest = await asyncio.to_thread(
                execute_bundle, snapshot=saved, destination=tmp_path / "verified-result"
            )
            assert manifest.event_count == 6 and manifest.independent_replay == "passed"
            original = FileReference(
                key=f"uploads/{dataset.original_digest}.csv",
                sha256=str(object=dataset.original_digest),
                size_bytes=len(source),
            )

            def read_original() -> bytes:
                with files.verified_stream(reference=original) as stream:
                    return stream.read()

            assert await asyncio.to_thread(read_original) == source

    asyncio.run(main=scenario())


def test_concurrent_idempotent_submissions_share_one_durable_job(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            files = await asyncio.to_thread(
                ManagedFiles, root=tmp_path, limits=StorageLimits(min_free_bytes=0)
            )
            service = Submissions(catalog=prepared.catalog, files=files)
            dataset = await service.upload(
                source=io.BytesIO(initial_bytes=b"t,request_id,work_units,mem_mb\n0,r,1,1\n"),
                filename="request.csv",
            )
            runs = await asyncio.gather(
                service.submit(dataset_id=dataset.id, settings=RunSettings(), key="same"),
                service.submit(dataset_id=dataset.id, settings=RunSettings(), key="same"),
            )
            assert runs[0] == runs[1]
            async with database.sessions() as session:
                assert await session.scalar(statement=select(func.count()).select_from(Job)) == 1
                assert await session.scalar(statement=select(func.count()).select_from(Outbox)) == 1

    asyncio.run(main=scenario())


def test_snapshot_storage_failure_does_not_register_a_job_or_replace_dataset(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            files = await asyncio.to_thread(
                ManagedFiles, root=tmp_path, limits=StorageLimits(min_free_bytes=0)
            )
            service = Submissions(catalog=prepared.catalog, files=files)
            dataset = await service.upload(
                source=io.BytesIO(initial_bytes=b"t,request_id,work_units,mem_mb\n0,r,1,1\n"),
                filename="request.csv",
            )
            await asyncio.to_thread((tmp_path / "snapshots").chmod, mode=0o500)
            try:
                with pytest.raises(expected_exception=PermissionError):
                    await service.submit(dataset_id=dataset.id, settings=RunSettings())
            finally:
                await asyncio.to_thread((tmp_path / "snapshots").chmod, mode=0o700)
            async with database.sessions() as session:
                assert await session.scalar(statement=select(func.count()).select_from(Job)) == 0
                assert await session.scalar(statement=select(func.count()).select_from(Run)) == 0
            assert await prepared.catalog.get_dataset(dataset_id=dataset.id) == dataset

    asyncio.run(main=scenario())


def test_reimport_supplies_real_source_provenance_for_legacy_metadata(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            assert prepared.dataset.original_digest is None
            files = await asyncio.to_thread(
                ManagedFiles, root=tmp_path, limits=StorageLimits(min_free_bytes=0)
            )
            service = Submissions(catalog=prepared.catalog, files=files)
            source = await asyncio.to_thread(Path("/fixtures/core/requests.csv").read_bytes)
            current = await service.upload(
                source=io.BytesIO(initial_bytes=source), filename="reimported.csv"
            )
            assert current.id == prepared.dataset.id
            assert current.original_digest == hashlib.sha256(data=source).hexdigest()
            assert current.source_size_bytes == len(source)
            async with database.engine.begin() as connection:
                with pytest.raises(expected_exception=IntegrityError):
                    await connection.execute(
                        statement=update(table=Dataset)
                        .where(Dataset.id == current.id)
                        .values(source_size_bytes=None)
                    )

    asyncio.run(main=scenario())

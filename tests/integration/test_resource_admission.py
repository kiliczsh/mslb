"""Real database admission, commit-time freshness and pressure-safe cleanup."""

import asyncio
import io
from datetime import UTC, datetime
from pathlib import Path
from threading import Event

import pytest
from mslb_application.errors import OperationError
from mslb_application.resource_policy import ResourcePolicy
from mslb_application.worker import Worker
from mslb_contracts.docker_runs import ContainerObservation
from mslb_contracts.runs import RunSettings
from mslb_persistence import resource_admission
from mslb_persistence.database import Database, StoreError
from mslb_persistence.durability import Durability
from mslb_persistence.jobs import Jobs
from mslb_persistence.models import (
    DockerPublication,
    IdempotencyRecord,
    Job,
    PublishedArtifact,
    Run,
    SavedCheckpoint,
)
from mslb_persistence.records import Lease
from mslb_persistence.resource_evidence import ResourceEvidenceStore
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from tests.integration.helpers import scratch
from tests.integration.persistence_helpers import OutputFixture
from tests.integration.persistence_helpers import prepare as prepare_persistence
from tests.integration.test_docker_control import claim, manifest, prepared
from tests.integration.test_queries import prepare
from tests.integration.test_resource_evidence import healthy_evidence
from tests.unit.platform.test_resource_policy import probe as synthetic_probe


def test_admission_holds_new_work_preserves_receipts_and_serializes_claims(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            _, service, runs = await prepare(
                database=database, root=tmp_path, settings=(RunSettings(),)
            )
            run = await service.catalog.get_run(run_id=runs[0])
            receipt = await service.submit(
                dataset_id=run.dataset_id, settings=RunSettings(), key="retained"
            )
            database.enforce_resource_guard = True
            assert (
                await service.submit(
                    dataset_id=run.dataset_id, settings=RunSettings(), key="retained"
                )
            ).id == receipt.id
            for operation in (
                service.submit(dataset_id=run.dataset_id, settings=RunSettings(), key="new"),
                service.upload(source=io.BytesIO(initial_bytes=b"invalid"), filename="new.csv"),
            ):
                with pytest.raises(expected_exception=OperationError) as failure:
                    await operation
                assert failure.value.code == "RESOURCE_PRESSURE"
            jobs = Jobs(database=database)
            assert await jobs.claim(owner="held") is None
            assert (await jobs.get(run_id=run.id)).attempts == 0
            store = ResourceEvidenceStore(database=database)
            publisher = await store.claim(owner="guard")
            assert publisher is not None
            probe, decision = healthy_evidence()
            await store.publish(
                lease=publisher,
                expected_revision=publisher.revision,
                probe=probe,
                decision=decision,
            )
            claims = await asyncio.gather(
                jobs.claim(owner="first"), jobs.claim(owner="second"), jobs.claim(owner="third")
            )
            assert sum(lease is not None for lease in claims) == 1

    asyncio.run(main=scenario())


@pytest.mark.parametrize(argnames="percent", argvalues=(70, 85))
def test_logical_publication_distinguishes_admission_hold_from_runtime_stop(
    tmp_path: Path, percent: int
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepare_persistence(database=database)
            run = await data.enqueue()
            lease = await data.jobs.claim(owner="finalizer")
            assert lease is not None
            flow = await asyncio.to_thread(OutputFixture, prepared=data, lease=lease, root=tmp_path)
            while not await asyncio.to_thread(flow.step):
                pass
            durable = Durability(database=database)
            await durable.commit(lease=lease, data=await asyncio.to_thread(flow.describe))
            candidate, bundle = await asyncio.to_thread(flow.candidate)
            store = ResourceEvidenceStore(database=database)
            publisher = await store.claim(owner="guard")
            assert publisher is not None
            probe = synthetic_probe(percent=percent).model_copy(
                update={"measured_at": datetime.now(tz=UTC)}
            )
            decision = ResourcePolicy().evaluate(probe=probe, now=probe.measured_at)
            assert not decision.admit and decision.stop_running is (percent == 85)
            await store.publish(
                lease=publisher,
                expected_revision=publisher.revision,
                probe=probe,
                decision=decision,
            )
            database.enforce_resource_guard = True
            if percent == 70:
                assert (
                    await durable.complete(lease=lease, reference=candidate, manifest=bundle)
                ).status == "completed"
            else:
                with pytest.raises(expected_exception=StoreError) as failure:
                    await durable.complete(lease=lease, reference=candidate, manifest=bundle)
                assert failure.value.code == "RESOURCE_PRESSURE"
                assert (
                    await data.jobs.fail(lease=lease, error_code="RESOURCE_PRESSURE")
                ).status == "failed"
            async with database.sessions() as session:
                assert await session.scalar(
                    statement=select(func.count()).select_from(PublishedArtifact)
                ) == int(percent == 70)
            assert (await data.jobs.get(run_id=run.id)).attempts == 1

    asyncio.run(main=scenario())


@pytest.mark.parametrize(argnames="percent", argvalues=(70, 85))
def test_docker_publication_cannot_cross_a_runtime_pressure_stop(percent: int) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            run = await data.enqueue()
            lease = await claim(data=data)
            cleanup = await data.control.begin_cleanup(lease=lease, outcome="completed")
            store = ResourceEvidenceStore(database=database)
            publisher = await store.claim(owner="guard")
            assert publisher is not None
            probe = synthetic_probe(percent=percent).model_copy(
                update={"measured_at": datetime.now(tz=UTC)}
            )
            decision = ResourcePolicy().evaluate(probe=probe, now=probe.measured_at)
            await store.publish(
                lease=publisher,
                expected_revision=publisher.revision,
                probe=probe,
                decision=decision,
            )
            database.enforce_resource_guard = True
            job = await data.control.finish_cleanup(
                lease=cleanup, manifest=manifest(run=run, lease=cleanup)
            )
            assert (job.status, job.error_code) == (
                ("completed", None) if percent == 70 else ("failed", "RESOURCE_PRESSURE")
            )
            async with database.sessions() as session:
                assert await session.scalar(
                    statement=select(func.count()).select_from(DockerPublication)
                ) == int(percent == 70)

    asyncio.run(main=scenario())


def test_commit_time_age_rolls_back_enqueue_and_idempotency(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            _, service, runs = await prepare(
                database=database, root=tmp_path, settings=(RunSettings(),)
            )
            run = await service.catalog.get_run(run_id=runs[0])
            store = ResourceEvidenceStore(database=database)
            publisher = await store.claim(owner="guard")
            assert publisher is not None
            probe, decision = healthy_evidence()
            await store.publish(
                lease=publisher,
                expected_revision=publisher.revision,
                probe=probe,
                decision=decision,
            )
            database.enforce_resource_guard = True
            original = resource_admission.admission_allowed
            delayed = False

            async def pause_after_admission(*, database: Database, session: AsyncSession) -> bool:
                nonlocal delayed
                allowed = await original(database=database, session=session)
                if allowed and not delayed:
                    delayed = True
                    await session.execute(statement=text(text="SELECT pg_sleep(3.05)"))
                return allowed

            monkeypatch.setattr(
                target=resource_admission, name="admission_allowed", value=pause_after_admission
            )
            with pytest.raises(expected_exception=StoreError) as failure:
                await service.submit(
                    dataset_id=run.dataset_id, settings=RunSettings(), key="stale-before-commit"
                )
            assert delayed and failure.value.code == "RESOURCE_PRESSURE"
            async with database.sessions() as session:
                assert await session.scalar(statement=select(func.count()).select_from(Run)) == 1
                assert await session.scalar(statement=select(func.count()).select_from(Job)) == 1
                assert (
                    await session.scalar(
                        statement=select(func.count()).select_from(IdempotencyRecord)
                    )
                    == 0
                )

    asyncio.run(main=scenario())


def test_running_worker_stops_on_pressure_without_publishing_or_retrying(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            csv = b"t,request_id,work_units,mem_mb\n" + b"".join(
                f"0,r{index},100,1\n".encode() for index in range(60)
            )
            outputs, service, runs = await prepare(
                database=database, root=tmp_path, settings=(RunSettings(),), requests=csv
            )
            store = ResourceEvidenceStore(database=database)
            publisher = await store.claim(owner="guard")
            assert publisher is not None
            probe, decision = healthy_evidence()
            evidence = await store.publish(
                lease=publisher,
                expected_revision=publisher.revision,
                probe=probe,
                decision=decision,
            )
            database.enforce_resource_guard = True
            entered, release = Event(), Event()

            def observer(phase: str, lease: Lease, tick: int) -> None:
                if phase == "checkpoint_committed" and not entered.is_set():
                    entered.set()
                    release.wait(timeout=5)

            worker = Worker(
                database=database, inputs=service.files, outputs=outputs, observer=observer
            )
            task = asyncio.create_task(coro=worker.run_once())
            try:
                assert await asyncio.to_thread(entered.wait, timeout=5)
                pressure = synthetic_probe(percent=85).model_copy(
                    update={"measured_at": datetime.now(tz=UTC)}
                )
                decision = ResourcePolicy().evaluate(probe=pressure, now=pressure.measured_at)
                assert decision.stop_running
                await store.publish(
                    lease=publisher,
                    expected_revision=evidence.revision,
                    probe=pressure,
                    decision=decision,
                )
                await asyncio.sleep(delay=1.1)
            finally:
                release.set()
                assert await asyncio.wait_for(fut=task, timeout=10)
            job = await Jobs(database=database).get(run_id=runs[0])
            assert (job.status, job.error_code, job.attempts) == ("failed", "RESOURCE_PRESSURE", 1)
            async with database.sessions() as session:
                checkpoints = await session.scalar(
                    statement=select(func.count()).select_from(SavedCheckpoint)
                )
                assert checkpoints is not None and checkpoints > 0
                assert (
                    await session.scalar(
                        statement=select(func.count()).select_from(PublishedArtifact)
                    )
                    == 0
                )

    asyncio.run(main=scenario())


def test_pressure_blocks_docker_growth_but_preserves_cleanup_authority() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            run = await data.enqueue()
            lease = await claim(data=data)
            intent = await data.resources.prepare_dynamic(lease=lease)
            database.enforce_resource_guard = True
            for operation in (
                data.resources.reserve_static(lease=lease, name="backend-a"),
                data.resources.prepare_dynamic(lease=lease),
                data.resources.authorize_create(lease=lease, resource_id=intent.id),
            ):
                with pytest.raises(expected_exception=StoreError) as failure:
                    await operation
                assert failure.value.code == "RESOURCE_PRESSURE"
            assert (
                await data.resources.get(lease=lease, resource_id=intent.id)
            ).state == "prepared"
            assert len(await data.resources.list(lease=lease)) == 1
            cleanup = await data.control.begin_cleanup(
                lease=lease, outcome="failed", error_code="RESOURCE_PRESSURE"
            )
            await data.resources.transition(lease=cleanup, resource_id=intent.id, action="abandon")
            await data.control.finish_cleanup(lease=cleanup)
            job = await Jobs(database=database).get(run_id=run.id)
            assert (job.status, job.error_code) == ("failed", "RESOURCE_PRESSURE")

    asyncio.run(main=scenario())


def test_pressure_cannot_start_an_already_created_container() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            await data.enqueue()
            lease = await claim(data=data)
            intent = await data.resources.prepare_dynamic(lease=lease)
            definition = await data.resources.authorize_create(lease=lease, resource_id=intent.id)
            await data.resources.observe_dynamic(
                lease=lease,
                resource_id=intent.id,
                observed=ContainerObservation(container_id="e" * 64, definition=definition),
            )
            database.enforce_resource_guard = True
            with pytest.raises(expected_exception=StoreError) as failure:
                await data.resources.transition(
                    lease=lease, resource_id=intent.id, action="authorize_start"
                )
            assert failure.value.code == "RESOURCE_PRESSURE"
            assert (
                await data.resources.get(lease=lease, resource_id=intent.id)
            ).state == "observed"
            cleanup = await data.control.begin_cleanup(
                lease=lease, outcome="failed", error_code="RESOURCE_PRESSURE"
            )
            for action in ("drain", "removed"):
                await data.resources.transition(lease=cleanup, resource_id=intent.id, action=action)
            await data.control.finish_cleanup(lease=cleanup)

    asyncio.run(main=scenario())

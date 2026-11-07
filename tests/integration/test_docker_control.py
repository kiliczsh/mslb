"""Real PostgreSQL mode isolation, fenced lifecycle, and interruption/cleanup races."""

import asyncio
import hashlib
from dataclasses import dataclass
from datetime import timedelta
from http import HTTPStatus
from uuid import uuid4

import pytest
from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_runs import (
    ContainerObservation,
    CreationIntent,
    DeploymentSpec,
    DockerManifest,
    DockerSettings,
    DockerSnapshot,
    StaticBackend,
)
from mslb_contracts.durability import StoredFile
from mslb_contracts.traffic import BackendHealth
from mslb_persistence import docker_control, docker_resources
from mslb_persistence.database import Database, StoreError
from mslb_persistence.docker_catalog import DockerCatalog
from mslb_persistence.docker_control import DockerControl
from mslb_persistence.docker_records import ControlLease
from mslb_persistence.docker_resources import DockerResources
from mslb_persistence.jobs import Jobs
from mslb_persistence.models import (
    DockerControlSlot,
    DockerPublication,
    DockerResource,
    IdempotencyRecord,
    Job,
    PublishedArtifact,
)
from mslb_persistence.reads import Reads
from mslb_persistence.records import Lease, RunRecord
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from tests.integration.helpers import scratch
from tests.integration.test_persistence import Prepared, prepare


@dataclass(frozen=True, slots=True)
class PreparedDocker:
    logical: Prepared
    catalog: DockerCatalog
    control: DockerControl
    resources: DockerResources
    snapshot: DockerSnapshot

    async def enqueue(self, key: str | None = None) -> RunRecord:
        return await self.catalog.enqueue(snapshot=self.snapshot, key=key)


async def prepared(database: Database, settings: DockerSettings | None = None) -> PreparedDocker:
    logical = await prepare(database=database)
    catalog = DockerCatalog(database=database)
    deployment = await catalog.enroll(
        definition=DeploymentSpec(
            image_id="sha256:" + "c" * 64,
            network_id="d" * 64,
            static=(
                StaticBackend(name="backend-a", container_id="a" * 64),
                StaticBackend(name="backend-b", container_id="b" * 64),
            ),
        )
    )
    snapshot = DockerSnapshot(
        dataset_id=logical.dataset.id,
        dataset_digest=logical.dataset.digest,
        request_count=logical.dataset.request_count,
        latest_arrival=logical.dataset.latest_arrival,
        deployment_id=deployment.id,
        deployment_digest=deployment.definition.digest(),
        configuration=settings or DockerSettings(),
    )
    return PreparedDocker(
        logical=logical,
        catalog=catalog,
        control=DockerControl(database=database),
        resources=DockerResources(database=database),
        snapshot=snapshot,
    )


async def claim(data: PreparedDocker, owner: str = "controller") -> ControlLease:
    lease = await data.control.claim(owner=owner)
    assert lease is not None
    return lease


async def expire(database: Database) -> None:
    async with database.engine.begin() as connection:
        await connection.execute(
            statement=update(table=DockerControlSlot)
            .where(DockerControlSlot.id == 1)
            .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
        )
        await connection.execute(
            statement=update(table=Job)
            .where(Job.kind == "docker", Job.status.in_(other=("running", "cancel_requested")))
            .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
        )


def manifest(run: RunRecord, lease: ControlLease, total: int = 4) -> DockerManifest:
    return DockerManifest(
        run_id=run.id,
        publication_fence=lease.fence,
        snapshot_digest=run.snapshot_digest,
        total=total,
        completed=total,
        failed=0,
        dropped=0,
        unexecuted=0,
        artifacts={
            name: StoredFile(
                key=f"docker/{run.id.hex}/{lease.fence}/{name}", sha256="e" * 64, size_bytes=1
            )
            for name in ("http_events.jsonl", "runtime_summary.json", "container_lifecycle.jsonl")
        },
    )


def idle(name: str) -> BackendHealth:
    return BackendHealth(
        backend_id=name,
        ready=True,
        logical_admission_mem_mb=1024,
        max_inflight=4,
        inflight=0,
        reserved_mem_mb=0,
        rss_bytes=None,
        cgroup_memory_bytes=None,
        cpu_usage_usec=None,
        throttled_usec=None,
    )


def test_docker_enqueue_is_atomic_idempotent_and_isolated_from_logical_claims_and_reads() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            first = await data.enqueue(key="same-docker-body")
            assert (await data.enqueue(key="same-docker-body")).id == first.id
            assert (await data.catalog.snapshot(run_id=first.id)).digest() == first.snapshot_digest
            with pytest.raises(expected_exception=StoreError, match="different request"):
                await data.catalog.enqueue(
                    snapshot=data.snapshot.model_copy(
                        update={"configuration": DockerSettings(strategy="round_robin")}
                    ),
                    key="same-docker-body",
                )
            assert await data.logical.jobs.claim(owner="logical-worker") is None
            with pytest.raises(expected_exception=StoreError) as wrong_cancel:
                await data.logical.jobs.cancel(run_id=first.id)
            assert wrong_cancel.value.code == "NOT_FOUND"
            assert (await data.logical.jobs.get(run_id=first.id)).status == "queued"
            assert await Reads(database=database).runs() == ()
            with pytest.raises(expected_exception=StoreError) as missing:
                await Reads(database=database).run(run_id=first.id)
            assert missing.value.code == "NOT_FOUND"
            logical = await data.logical.enqueue(key="logical-body")
            logical_lease = await data.logical.jobs.claim(owner="logical-worker")
            assert logical_lease is not None and logical_lease.run_id == logical.id
            selected = await claim(data=data)
            assert selected.run_id == first.id and selected.phase == "running"
            with pytest.raises(expected_exception=StoreError) as wrong_adapter:
                await data.logical.jobs.renew(
                    lease=Lease(
                        run_id=selected.run_id,
                        attempt=1,
                        fence=selected.fence,
                        owner=selected.owner,
                        expires_at=selected.expires_at,
                    )
                )
            assert wrong_adapter.value.code == "FENCE_LOST"
            assert len(await data.logical.jobs.pending_outbox()) == 2

    asyncio.run(main=scenario())


def test_racing_controllers_get_one_owner_and_stale_control_cannot_reserve_or_finalize() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            run = await data.enqueue()
            raced = await asyncio.gather(
                data.control.claim(owner="first"), data.control.claim(owner="second")
            )
            owners = [lease for lease in raced if lease is not None]
            assert len(owners) == 1
            lease = owners[0]
            renewed = await data.control.renew(lease=lease, lease_seconds=60)
            assert renewed.expires_at > lease.expires_at
            await expire(database=database)
            recovering = await claim(data=data, owner="recovery")
            assert recovering.phase == "cleanup" and recovering.fence > lease.fence
            assert (await Jobs(database=database).get(run_id=run.id)).status == "failed"
            assert (await Jobs(database=database).get(run_id=run.id)).error_code == "INTERRUPTED"
            for operation in (
                data.resources.reserve_static(lease=lease, name="backend-a"),
                data.control.renew(lease=lease),
                data.control.finish_cleanup(lease=lease, manifest=manifest(run=run, lease=lease)),
            ):
                with pytest.raises(expected_exception=StoreError) as stale:
                    await operation
                assert stale.value.code == "FENCE_LOST"
            result = await data.control.finish_cleanup(lease=recovering)
            assert result.status == "failed" and result.attempts == 1

    asyncio.run(main=scenario())


def test_reservations_are_bounded_and_ambiguous_creation_is_not_retried() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            await data.enqueue()
            lease = await claim(data=data)
            static_a = await data.resources.reserve_static(lease=lease, name="backend-a")
            assert (
                await data.resources.reserve_static(lease=lease, name="backend-a")
            ).id == static_a.id
            await data.resources.reserve_static(lease=lease, name="backend-b")
            identity = uuid4()
            first = await data.resources.prepare_dynamic(lease=lease, intent_id=identity)
            assert (
                await data.resources.prepare_dynamic(lease=lease, intent_id=identity)
            ).id == first.id
            second = await data.resources.prepare_dynamic(lease=lease)
            assert first.name != second.name and first.slot != second.slot
            with pytest.raises(expected_exception=StoreError) as full:
                await data.resources.prepare_dynamic(lease=lease)
            assert full.value.code == "RATE_LIMITED"
            created = await data.resources.authorize_create(lease=lease, resource_id=first.id)
            assert created.name == first.name
            with pytest.raises(expected_exception=StoreError):
                await data.resources.authorize_create(lease=lease, resource_id=first.id)
            await data.resources.transition(lease=lease, resource_id=first.id, action="uncertain")
            with pytest.raises(expected_exception=StoreError):
                await data.resources.transition(lease=lease, resource_id=first.id, action="abandon")
            for status in (
                HTTPStatus.CONFLICT.value,
                HTTPStatus.INTERNAL_SERVER_ERROR.value,
                HTTPStatus.GATEWAY_TIMEOUT.value,
            ):
                with pytest.raises(expected_exception=StoreError):
                    await data.resources.reject_create(
                        lease=lease, resource_id=first.id, daemon_status=status
                    )
            assert (await data.resources.list(lease=lease))[2].state == "unresolved"

    asyncio.run(main=scenario())


def test_conflicting_inspection_never_authorizes_unrelated_container_removal() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            await data.enqueue()
            lease = await claim(data=data)
            resource = await data.resources.prepare_dynamic(lease=lease)
            definition = await data.resources.authorize_create(lease=lease, resource_id=resource.id)
            wrong = definition.model_copy(update={"network_id": "f" * 64})
            conflict = await data.resources.observe_dynamic(
                lease=lease,
                resource_id=resource.id,
                observed=ContainerObservation(container_id="1" * 64, definition=wrong),
            )
            assert conflict.state == "conflict" and conflict.container_id is None
            with pytest.raises(expected_exception=StoreError):
                await data.resources.transition(
                    lease=lease, resource_id=resource.id, action="drain"
                )
            adopted = await data.resources.observe_dynamic(
                lease=lease,
                resource_id=resource.id,
                observed=ContainerObservation(container_id="2" * 64, definition=definition),
            )
            assert adopted.state == "observed" and adopted.container_id == "2" * 64
            peer = await data.resources.prepare_dynamic(lease=lease)
            peer_definition = await data.resources.authorize_create(
                lease=lease, resource_id=peer.id
            )
            duplicate = await data.resources.observe_dynamic(
                lease=lease,
                resource_id=peer.id,
                observed=ContainerObservation(container_id="2" * 64, definition=peer_definition),
            )
            assert duplicate.state == "conflict" and duplicate.container_id is None
            await data.resources.transition(
                lease=lease, resource_id=resource.id, action="authorize_start"
            )
            await data.resources.transition(lease=lease, resource_id=resource.id, action="started")
            wrong_id = await data.resources.observe_dynamic(
                lease=lease,
                resource_id=resource.id,
                observed=ContainerObservation(container_id="3" * 64, definition=definition),
            )
            assert wrong_id.state == "conflict" and wrong_id.container_id == "2" * 64

    asyncio.run(main=scenario())


@pytest.mark.parametrize(
    argnames="crash_phase", argvalues=["prepared", "creating", "observed", "starting"]
)
def test_interruption_reconciles_existing_intent_without_replaying_work_or_freeing_capacity(
    crash_phase: str,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            run = await data.enqueue()
            next_run = await data.enqueue()
            lease = await claim(data=data)
            resource = await data.resources.prepare_dynamic(lease=lease)
            assert isinstance(resource.definition, CreationIntent)
            definition = resource.definition
            if crash_phase != "prepared":
                definition = await data.resources.authorize_create(
                    lease=lease, resource_id=resource.id
                )
            if crash_phase in {"observed", "starting"}:
                await data.resources.observe_dynamic(
                    lease=lease,
                    resource_id=resource.id,
                    observed=ContainerObservation(container_id="4" * 64, definition=definition),
                )
            if crash_phase == "starting":
                await data.resources.transition(
                    lease=lease, resource_id=resource.id, action="authorize_start"
                )
            await expire(database=database)
            cleanup = await claim(data=data, owner="replacement")
            assert cleanup.run_id == run.id and cleanup.phase == "cleanup"
            assert await data.control.claim(owner="other") is None
            with pytest.raises(expected_exception=StoreError):
                await data.resources.prepare_dynamic(lease=cleanup)
            with pytest.raises(expected_exception=StoreError):
                await data.control.finish_cleanup(lease=cleanup)
            if crash_phase == "prepared":
                await data.resources.transition(
                    lease=cleanup, resource_id=resource.id, action="abandon"
                )
            else:
                # Inspect/adopt the same frozen intent after a lost daemon response.
                observed = await data.resources.observe_dynamic(
                    lease=cleanup,
                    resource_id=resource.id,
                    observed=ContainerObservation(container_id="4" * 64, definition=definition),
                )
                assert observed.container_id == "4" * 64 and observed.name == resource.name
                await data.resources.transition(
                    lease=cleanup, resource_id=resource.id, action="drain"
                )
                await data.resources.transition(
                    lease=cleanup, resource_id=resource.id, action="removed"
                )
            result = await data.control.finish_cleanup(lease=cleanup)
            assert result.status == "failed" and result.attempts == 1
            assert (await claim(data=data, owner="next-owner")).run_id == next_run.id
            with pytest.raises(expected_exception=StoreError):
                await data.resources.observe_dynamic(
                    lease=lease,
                    resource_id=resource.id,
                    observed=ContainerObservation(container_id="4" * 64, definition=definition),
                )

    asyncio.run(main=scenario())


def test_cleanup_requires_idle_static_backends_and_a_separate_completed_manifest() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            run = await data.enqueue()
            lease = await claim(data=data)
            backend = await data.resources.reserve_static(lease=lease, name="backend-a")
            cleanup = await data.control.begin_cleanup(lease=lease, outcome="completed")
            with pytest.raises(expected_exception=StoreError):
                await data.control.finish_cleanup(
                    lease=cleanup, manifest=manifest(run=run, lease=cleanup)
                )
            await data.resources.transition(lease=cleanup, resource_id=backend.id, action="drain")
            with pytest.raises(expected_exception=StoreError):
                await data.resources.release_static(
                    lease=cleanup,
                    resource_id=backend.id,
                    container_id="a" * 64,
                    observed=idle(name="backend-a").model_copy(update={"inflight": 1}),
                )
            with pytest.raises(expected_exception=StoreError):
                await data.resources.release_static(
                    lease=cleanup,
                    resource_id=backend.id,
                    container_id="b" * 64,
                    observed=idle(name="backend-a"),
                )
            await data.resources.release_static(
                lease=cleanup,
                resource_id=backend.id,
                container_id="a" * 64,
                observed=idle(name="backend-a"),
            )
            with pytest.raises(expected_exception=StoreError):
                await data.control.finish_cleanup(lease=cleanup)
            with pytest.raises(expected_exception=StoreError):
                await data.control.finish_cleanup(
                    lease=cleanup, manifest=manifest(run=run, lease=cleanup, total=3)
                )
            result = await data.control.finish_cleanup(
                lease=cleanup, manifest=manifest(run=run, lease=cleanup)
            )
            assert result.status == "completed"
            async with database.sessions() as session:
                assert await session.get(entity=DockerPublication, ident=run.id) is not None
                assert await session.get(entity=PublishedArtifact, ident=run.id) is None
            second = await data.enqueue()
            new_lease = await claim(data=data, owner="next")
            assert new_lease.run_id == second.id
            assert (
                await data.resources.reserve_static(lease=new_lease, name="backend-a")
            ).container_id == "a" * 64

    asyncio.run(main=scenario())


def test_cancellation_wins_during_cleanup_and_cannot_publish_an_artifact() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            run = await data.enqueue()
            lease = await claim(data=data)
            cleanup = await data.control.begin_cleanup(lease=lease, outcome="completed")
            await data.control.cancel(run_id=run.id)
            result = await data.control.finish_cleanup(
                lease=cleanup, manifest=manifest(run=run, lease=cleanup)
            )
            assert result.status == "cancelled"
            async with database.sessions() as session:
                assert await session.get(entity=DockerPublication, ident=run.id) is None

    asyncio.run(main=scenario())


def test_automatic_cleanup_retries_are_bounded_and_explicit_retry_does_not_release_resources() -> (
    None
):
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            run = await data.enqueue()
            lease = await claim(data=data)
            resource = await data.resources.prepare_dynamic(lease=lease)
            await data.resources.authorize_create(lease=lease, resource_id=resource.id)
            for number in range(3):
                await expire(database=database)
                cleanup = await data.control.claim(owner=f"cleanup-{number}")
                assert cleanup is not None and cleanup.phase == "cleanup"
            await expire(database=database)
            assert await data.control.claim(owner="too-many") is None
            await data.control.retry_cleanup(run_id=run.id)
            retried = await claim(data=data, owner="operator-requested")
            assert retried.phase == "cleanup" and len(await data.resources.list(lease=retried)) == 1
            with pytest.raises(expected_exception=StoreError):
                await data.control.finish_cleanup(lease=retried)
            await data.resources.reject_create(
                lease=retried, resource_id=resource.id, daemon_status=HTTPStatus.NOT_FOUND.value
            )
            assert (await data.control.finish_cleanup(lease=retried)).status == "failed"

    asyncio.run(main=scenario())


def test_expiry_at_final_mutation_rolls_back_prepared_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = docker_resources.guard

    async def delayed_guard(session: AsyncSession, lease: ControlLease) -> DockerControlSlot:
        await asyncio.sleep(delay=1.05)
        return await original(session=session, lease=lease)

    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            run = await data.enqueue()
            lease = await data.control.claim(owner="expiring", lease_seconds=1)
            assert lease is not None
            with monkeypatch.context() as patch:
                patch.setattr(target=docker_resources, name="guard", value=delayed_guard)
                with pytest.raises(expected_exception=StoreError) as expired:
                    await data.resources.prepare_dynamic(lease=lease)
                assert expired.value.code == "FENCE_LOST"
            async with database.sessions() as session:
                assert (
                    await session.scalar(
                        statement=select(func.count())
                        .select_from(DockerResource)
                        .where(DockerResource.run_id == run.id)
                    )
                    == 0
                )
            cleanup = await claim(data=data, owner="reconciler")
            assert cleanup.phase == "cleanup"
            assert (await data.control.finish_cleanup(lease=cleanup)).status == "failed"

    asyncio.run(main=scenario())


def test_resource_history_is_finite_without_preventing_cleanup() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            await data.enqueue()
            lease = await claim(data=data)
            names = set()
            for _ in range(64):
                resource = await data.resources.prepare_dynamic(lease=lease)
                names.add(resource.name)
                await data.resources.transition(
                    lease=lease, resource_id=resource.id, action="abandon"
                )
            assert len(names) == 64
            with pytest.raises(expected_exception=StoreError) as bounded:
                await data.resources.prepare_dynamic(lease=lease)
            assert bounded.value.code == "RESOURCE_LIMIT_EXCEEDED"
            cleanup = await data.control.begin_cleanup(
                lease=lease, outcome="failed", error_code="RESOURCE_LIMIT_EXCEEDED"
            )
            assert (await data.control.finish_cleanup(lease=cleanup)).status == "failed"

    asyncio.run(main=scenario())


def test_logical_and_docker_jobs_share_one_twenty_job_pending_budget() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            for _ in range(10):
                await data.logical.enqueue()
                await data.enqueue()
            for submission in (data.enqueue(), data.logical.enqueue()):
                with pytest.raises(expected_exception=StoreError) as full:
                    await submission
                assert full.value.code == "RATE_LIMITED"
            assert len(await data.logical.jobs.pending_outbox()) == 20

    asyncio.run(main=scenario())


def test_idempotency_expires_and_never_returns_a_run_through_the_wrong_mode() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            first = await data.enqueue(key="expires")
            with pytest.raises(expected_exception=StoreError) as conflict:
                await data.logical.enqueue(key="expires")
            assert conflict.value.code == "CONFLICT"
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=update(table=IdempotencyRecord).values(
                        expires_at=func.clock_timestamp() - timedelta(seconds=1)
                    )
                )
            changed = data.snapshot.model_copy(
                update={"configuration": DockerSettings(strategy="round_robin")}
            )
            second = await data.catalog.enqueue(snapshot=changed, key="expires")
            assert second.id != first.id
            digest = hashlib.sha256(
                data=canonical_bytes(
                    value={
                        "dataset_id": str(object=changed.dataset_id),
                        "settings": changed.configuration.model_dump(mode="json"),
                    }
                )
            ).hexdigest()
            with pytest.raises(expected_exception=StoreError) as wrong_mode:
                await data.logical.catalog.find_idempotent(key="expires", body_digest=digest)
            assert wrong_mode.value.code == "CONFLICT"

    asyncio.run(main=scenario())


def test_final_publication_rolls_back_if_its_lease_expires(monkeypatch: pytest.MonkeyPatch) -> None:
    original = docker_control.guard

    async def delayed_guard(
        session: AsyncSession, lease: ControlLease, release: bool = False
    ) -> DockerControlSlot:
        await asyncio.sleep(delay=1.05)
        return await original(session=session, lease=lease, release=release)

    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            run = await data.enqueue()
            lease = await data.control.claim(owner="finalizer", lease_seconds=1)
            assert lease is not None
            cleanup = await data.control.begin_cleanup(lease=lease, outcome="completed")
            with monkeypatch.context() as patch:
                patch.setattr(target=docker_control, name="guard", value=delayed_guard)
                with pytest.raises(expected_exception=StoreError) as expired:
                    await data.control.finish_cleanup(
                        lease=cleanup, manifest=manifest(run=run, lease=cleanup)
                    )
                assert expired.value.code == "FENCE_LOST"
            assert (await Jobs(database=database).get(run_id=run.id)).status == "running"
            async with database.sessions() as session:
                assert await session.get(entity=DockerPublication, ident=run.id) is None
            recovered = await claim(data=data, owner="replacement")
            assert (await data.control.finish_cleanup(lease=recovered)).status == "failed"

    asyncio.run(main=scenario())


def test_unassigned_static_backends_still_consume_physical_capacity() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            await data.enqueue()
            lease = await claim(data=data)
            await data.resources.prepare_dynamic(lease=lease)
            await data.resources.prepare_dynamic(lease=lease)
            assert len(await data.resources.list(lease=lease)) == 2
            with pytest.raises(expected_exception=StoreError) as bounded:
                await data.resources.prepare_dynamic(lease=lease)
            assert bounded.value.code == "RATE_LIMITED"
            await data.resources.reserve_static(lease=lease, name="backend-a")
            await data.resources.reserve_static(lease=lease, name="backend-b")
            assert len(await data.resources.list(lease=lease)) == 4

    asyncio.run(main=scenario())


def test_operator_retry_requires_expired_exhausted_cleanup_and_preserves_fencing() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            run = await data.enqueue()
            lease = await claim(data=data)
            intent = await data.resources.prepare_dynamic(lease=lease)
            with pytest.raises(expected_exception=StoreError):
                await data.control.retry_cleanup(run_id=run.id)
            lease = await data.control.begin_cleanup(
                lease=lease, outcome="failed", error_code="CONTROLLER_FAILURE"
            )
            for index in range(2):
                await expire(database=database)
                recovered = await data.control.claim(owner=f"recover-{index}")
                assert recovered is not None
                lease = recovered
            await expire(database=database)
            assert await data.control.claim(owner="automatic-budget-exhausted") is None
            with pytest.raises(expected_exception=StoreError):
                await data.control.retry_cleanup(run_id=uuid4())
            await data.control.retry_cleanup(run_id=run.id)
            with pytest.raises(expected_exception=StoreError) as stale:
                await data.resources.get(lease=lease, resource_id=intent.id)
            assert stale.value.code == "FENCE_LOST"
            recovered = await data.control.claim(owner="operator-retry")
            assert (
                recovered is not None
                and recovered.phase == "cleanup"
                and recovered.fence > lease.fence
            )
            assert (
                await data.resources.get(lease=recovered, resource_id=intent.id)
            ).state == "prepared"
            await data.resources.transition(
                lease=recovered, resource_id=intent.id, action="abandon"
            )
            finished = await data.control.finish_cleanup(lease=recovered)
            assert finished.status == "failed" and finished.error_code == "INTERRUPTED"

    asyncio.run(main=scenario())

"""Fenced static reservations and durable dynamic creation/cleanup intentions."""

from http import HTTPStatus
from uuid import UUID, uuid4

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_ownership import StoppedStatic
from mslb_contracts.docker_runs import (
    ContainerObservation,
    CreationIntent,
    DeploymentSpec,
    DockerSnapshot,
    StaticReservation,
)
from mslb_contracts.traffic import BackendHealth
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from mslb_persistence.database import Database
from mslb_persistence.docker_control import RESOLVED, current, execution, guard
from mslb_persistence.docker_models import DockerDeployment, DockerResource, DockerRunState
from mslb_persistence.docker_records import ControlLease, ResourceRecord, resource_record
from mslb_persistence.errors import StoreError
from mslb_persistence.resource_admission import require_admission


async def _population(session: AsyncSession, run_id: UUID) -> tuple[DockerResource, ...]:
    rows = tuple(
        await session.scalars(
            statement=select(DockerResource)
            .where(DockerResource.run_id == run_id)
            .order_by(DockerResource.ordinal)
            .limit(limit=65)
        )
    )
    if len(rows) > 64:
        raise StoreError(code="VALIDATION_FAILED", message="Resource history exceeds its envelope")
    return rows


async def _row(session: AsyncSession, lease: ControlLease, resource_id: UUID) -> DockerResource:
    row = await session.get(entity=DockerResource, ident=resource_id)
    if row is None or row.run_id != lease.run_id:
        raise StoreError(code="NOT_FOUND", message="Owned resource does not exist")
    resource_record(
        row=row
    )  # Verify stored definition before it authorizes any lifecycle transition.
    return row


def _capacity(
    rows: tuple[DockerResource, ...], state: DockerRunState
) -> tuple[int, int, DockerSnapshot]:
    snapshot = DockerSnapshot.model_validate_json(json_data=canonical_bytes(value=state.snapshot))
    active = [row for row in rows if row.state not in RESOLVED]
    if len(rows) >= 64:
        raise StoreError(code="RESOURCE_LIMIT_EXCEEDED", message="Resource history limit reached")
    if len(active) >= snapshot.configuration.max_replicas:
        raise StoreError(code="RATE_LIMITED", message="Replica reservation limit reached")
    occupied = {row.slot for row in active}
    slot = next(
        index for index in range(snapshot.configuration.max_replicas) if index not in occupied
    )
    return len(rows) + 1, slot, snapshot


class DockerResources:
    """Persist fenced static reservations and dynamic intents before daemon mutations."""

    def __init__(self, database: Database) -> None:
        self.database = database

    async def get(self, lease: ControlLease, resource_id: UUID) -> ResourceRecord:
        """Require a current fenced lease before reading one owned resource intent."""

        async def operation(session: AsyncSession) -> ResourceRecord:
            await current(session=session, lease=lease)
            return resource_record(
                row=await _row(session=session, lease=lease, resource_id=resource_id)
            )

        return await self.database.transaction(operation=operation)

    async def confirm_absent(
        self, lease: ControlLease, resource_id: UUID, container_id: str
    ) -> ResourceRecord:
        """Trusted adapter supplies an actual 404 for the already persisted immutable ID."""

        async def operation(session: AsyncSession) -> ResourceRecord:
            await current(session=session, lease=lease)
            row = await _row(session=session, lease=lease, resource_id=resource_id)
            if row.container_id is None or row.container_id != container_id:
                raise StoreError(
                    code="INVALID_STATE", message="Absence requires an already owned immutable ID"
                )
            if row.state not in {"observed", "starting", "started", "draining"}:
                raise StoreError(code="INVALID_STATE", message="Resource does not await cleanup")
            row.state = "removed" if row.kind == "dynamic" else "released"
            await guard(session=session, lease=lease)
            return resource_record(row=row)

        return await self.database.transaction(operation=operation)

    async def list(self, lease: ControlLease) -> tuple[ResourceRecord, ...]:
        """Require current lease ownership before reading the run resource inventory."""

        async def operation(session: AsyncSession) -> tuple[ResourceRecord, ...]:
            await current(session=session, lease=lease)
            return tuple(
                resource_record(row=row)
                for row in await _population(session=session, run_id=lease.run_id)
            )

        return await self.database.transaction(operation=operation)

    async def reserve_static(self, lease: ControlLease, name: str) -> ResourceRecord:
        """Reserve verified static backends exclusively under the active control fence."""

        async def operation(session: AsyncSession) -> ResourceRecord:
            _slot, job, state = await current(session=session, lease=lease)
            execution(lease=lease, job=job)
            deployment = await session.get(entity=DockerDeployment, ident=state.deployment_id)
            if deployment is None:
                raise StoreError(code="NOT_READY", message="Deployment is missing")
            definition = DeploymentSpec.model_validate_json(
                json_data=canonical_bytes(value=deployment.definition)
            )
            if definition.digest() != deployment.fingerprint:
                raise StoreError(
                    code="VALIDATION_FAILED", message="Deployment identity is inconsistent"
                )
            backend = next((item for item in definition.static if item.name == name), None)
            if backend is None:
                raise StoreError(
                    code="NOT_FOUND", message="Dedicated static backend does not exist"
                )
            rows = await _population(session=session, run_id=lease.run_id)
            for existing in rows:
                if (
                    existing.kind == "static"
                    and existing.name == name
                    and existing.state not in RESOLVED
                ):
                    if existing.state != "observed":
                        raise StoreError(code="INVALID_STATE", message="Static backend is draining")
                    return resource_record(row=existing)
            ordinal, index, _snapshot = _capacity(rows=rows, state=state)
            await require_admission(database=self.database, session=session)
            resource_id = uuid4()
            reserved = StaticReservation(
                deployment_id=deployment.id,
                run_id=lease.run_id,
                resource_id=resource_id,
                slot=index,
                backend=backend,
            )
            row = DockerResource(
                id=resource_id,
                run_id=lease.run_id,
                ordinal=ordinal,
                slot=index,
                kind="static",
                state="observed",
                name=name,
                container_id=backend.container_id,
                definition=reserved.model_dump(mode="json"),
                definition_digest=reserved.digest(),
            )
            session.add(instance=row)
            await guard(session=session, lease=lease)
            await require_admission(database=self.database, session=session)
            return resource_record(row=row)

        return await self.database.transaction(operation=operation)

    async def prepare_dynamic(
        self, lease: ControlLease, intent_id: UUID | None = None
    ) -> ResourceRecord:
        """Reserve a slot and immutable creation intent before any daemon create call."""
        stable_id = intent_id or uuid4()

        async def operation(session: AsyncSession) -> ResourceRecord:
            _slot, job, state = await current(session=session, lease=lease)
            execution(lease=lease, job=job)
            previous = await session.get(entity=DockerResource, ident=stable_id)
            if previous is not None:
                if previous.run_id != lease.run_id or previous.kind != "dynamic":
                    raise StoreError(
                        code="CONFLICT", message="Intent identity belongs to another resource"
                    )
                return resource_record(row=previous)
            rows = await _population(session=session, run_id=lease.run_id)
            # Two Compose backends consume physical budget even while unassigned.
            if sum(row.kind == "dynamic" and row.state not in RESOLVED for row in rows) >= 2:
                raise StoreError(
                    code="RATE_LIMITED", message="Aggregate physical backend budget reached"
                )
            ordinal, index, snapshot = _capacity(rows=rows, state=state)
            await require_admission(database=self.database, session=session)
            deployment = await session.get(entity=DockerDeployment, ident=state.deployment_id)
            if deployment is None:
                raise StoreError(code="NOT_READY", message="Deployment is missing")
            spec = DeploymentSpec.model_validate_json(
                json_data=canonical_bytes(value=deployment.definition)
            )
            if spec.digest() != deployment.fingerprint:
                raise StoreError(
                    code="VALIDATION_FAILED", message="Deployment identity is inconsistent"
                )
            settings = snapshot.configuration
            intent = CreationIntent(
                deployment_id=deployment.id,
                run_id=lease.run_id,
                intent_id=stable_id,
                creator_fence=lease.fence,
                slot=index,
                image_id=spec.image_id,
                network_id=spec.network_id,
                logical_admission_mem_mb=settings.logical_admission_mem_mb,
                max_inflight=settings.backend_inflight,
                memory_mb=settings.backend_memory_mb,
                nano_cpus=settings.backend_nano_cpus,
            )
            row = DockerResource(
                id=stable_id,
                run_id=lease.run_id,
                ordinal=ordinal,
                slot=index,
                kind="dynamic",
                state="prepared",
                name=intent.name,
                container_id=None,
                definition=intent.model_dump(mode="json"),
                definition_digest=intent.digest(),
            )
            session.add(instance=row)
            await guard(session=session, lease=lease)
            await require_admission(database=self.database, session=session)
            return resource_record(row=row)

        return await self.database.transaction(operation=operation)

    async def authorize_create(self, lease: ControlLease, resource_id: UUID) -> CreationIntent:
        """One grant per intent. An ambiguous response requires lookup, never another create."""

        async def operation(session: AsyncSession) -> CreationIntent:
            _slot, job, _state = await current(session=session, lease=lease)
            execution(lease=lease, job=job)
            row = await _row(session=session, lease=lease, resource_id=resource_id)
            record = resource_record(row=row)
            if (
                row.kind != "dynamic"
                or row.state != "prepared"
                or not isinstance(record.definition, CreationIntent)
            ):
                raise StoreError(
                    code="INVALID_STATE",
                    message="Creation was already authorized or is not dynamic",
                )
            await require_admission(database=self.database, session=session)
            row.state = "creating"
            await guard(session=session, lease=lease)
            await require_admission(database=self.database, session=session)
            return record.definition

        return await self.database.transaction(operation=operation)

    async def observe_dynamic(
        self, lease: ControlLease, resource_id: UUID, observed: ContainerObservation
    ) -> ResourceRecord:
        """Adopt an inspected immutable ID only when it matches the trusted intent."""

        async def operation(session: AsyncSession) -> ResourceRecord:
            await current(session=session, lease=lease)
            row = await _row(session=session, lease=lease, resource_id=resource_id)
            record = resource_record(row=row)
            if row.kind != "dynamic" or row.state in {"prepared", "removed", "abandoned"}:
                raise StoreError(
                    code="INVALID_STATE", message="Resource is not awaiting a daemon observation"
                )
            conflict = observed.definition != record.definition or (
                row.container_id is not None and row.container_id != observed.container_id
            )
            duplicate = await session.scalar(
                statement=select(DockerResource.id)
                .where(
                    DockerResource.id != row.id,
                    DockerResource.container_id == observed.container_id,
                    DockerResource.state.not_in(other=RESOLVED),
                )
                .limit(limit=1)
            )
            if conflict or duplicate is not None:
                row.state = "conflict"  # Never record an unrelated observed ID as owned.
            else:
                row.container_id = observed.container_id
                if row.state not in {"starting", "started", "draining"}:
                    row.state = "observed"
            await guard(session=session, lease=lease)
            return resource_record(row=row)

        return await self.database.transaction(operation=operation)

    async def transition(
        self, lease: ControlLease, resource_id: UUID, action: str
    ) -> ResourceRecord:
        """Only enumerated lifecycle transitions; daemon proofs are supplied by trusted adapters."""

        async def operation(session: AsyncSession) -> ResourceRecord:
            _slot, job, _state = await current(session=session, lease=lease)
            row = await _row(session=session, lease=lease, resource_id=resource_id)
            if action == "abandon" and row.kind == "dynamic" and row.state == "prepared":
                row.state = "abandoned"
            elif action == "uncertain" and row.kind == "dynamic" and row.state == "creating":
                row.state = "unresolved"
            elif action == "authorize_start" and row.kind == "dynamic" and row.state == "observed":
                execution(lease=lease, job=job)
                await require_admission(database=self.database, session=session)
                row.state = "starting"
            elif action == "started" and row.kind == "dynamic" and row.state == "starting":
                execution(lease=lease, job=job)
                row.state = "started"
            elif (
                action == "drain"
                and row.state in {"observed", "starting", "started"}
                and row.container_id is not None
            ):
                row.state = "draining"
            elif (
                action == "removed"
                and row.kind == "dynamic"
                and row.state == "draining"
                and row.container_id is not None
            ):
                row.state = "removed"
            else:
                raise StoreError(
                    code="INVALID_STATE",
                    message="Lifecycle transition requires current owned state",
                )
            await guard(session=session, lease=lease)
            if action == "authorize_start":
                await require_admission(database=self.database, session=session)
            return resource_record(row=row)

        return await self.database.transaction(operation=operation)

    async def release_static(
        self, lease: ControlLease, resource_id: UUID, container_id: str, observed: BackendHealth
    ) -> ResourceRecord:
        """Release the exact owned static reservation after lifecycle resolution."""

        async def operation(session: AsyncSession) -> ResourceRecord:
            await current(session=session, lease=lease)
            row = await _row(session=session, lease=lease, resource_id=resource_id)
            if row.kind != "static" or row.state != "draining" or row.container_id != container_id:
                raise StoreError(
                    code="INVALID_STATE",
                    message="Static release requires its exact draining identity",
                )
            if (
                observed.backend_id != row.name
                or observed.inflight != 0
                or observed.reserved_mem_mb != 0
            ):
                raise StoreError(
                    code="INVALID_STATE", message="Static backend is not observed idle"
                )
            row.state = "released"
            await guard(session=session, lease=lease)
            return resource_record(row=row)

        return await self.database.transaction(operation=operation)

    async def release_stopped_static(
        self, *, lease: ControlLease, resource_id: UUID, observed: StoppedStatic
    ) -> ResourceRecord:
        """Release only the current draining identity after trusted stopped-state inspection."""
        observed = StoppedStatic.model_validate_json(
            json_data=canonical_bytes(value=observed.model_dump(mode="json"))
        )

        async def operation(session: AsyncSession) -> ResourceRecord:
            await current(session=session, lease=lease)
            row = await _row(session=session, lease=lease, resource_id=resource_id)
            actual = resource_record(row=row)
            if (
                row.kind != "static"
                or row.state != "draining"
                or row.container_id != observed.container_id
                or actual.definition != observed.definition
            ):
                raise StoreError(
                    code="INVALID_STATE",
                    message="Stopped static release requires its exact draining reservation",
                )
            row.state = "released"
            await guard(session=session, lease=lease)
            return resource_record(row=row)

        return await self.database.transaction(operation=operation)

    async def reject_create(
        self, lease: ControlLease, resource_id: UUID, daemon_status: int
    ) -> ResourceRecord:
        """Only completed definitive rejection receipts resolve a sent create without an ID."""
        if type(daemon_status) is not int or daemon_status not in {
            HTTPStatus.BAD_REQUEST.value,
            HTTPStatus.FORBIDDEN.value,
            HTTPStatus.NOT_FOUND.value,
            HTTPStatus.UNPROCESSABLE_CONTENT.value,
        }:
            raise StoreError(
                code="VALIDATION_ERROR",
                message="Timeout, conflict or server failure cannot prove absence",
            )

        async def operation(session: AsyncSession) -> ResourceRecord:
            await current(session=session, lease=lease)
            row = await _row(session=session, lease=lease, resource_id=resource_id)
            if (
                row.kind != "dynamic"
                or row.state not in {"creating", "unresolved"}
                or row.container_id is not None
            ):
                raise StoreError(
                    code="INVALID_STATE", message="Creation has no unresolved rejection receipt"
                )
            row.state = "abandoned"
            await guard(session=session, lease=lease)
            return resource_record(row=row)

        return await self.database.transaction(operation=operation)

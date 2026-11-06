"""Trusted lifecycle orchestration: every action derives from a current persisted intent."""

from http import HTTPStatus
from uuid import UUID

from mslb_contracts.docker_ownership import ContainerObservation, CreationIntent, StaticReservation
from mslb_persistence.database import Database
from mslb_persistence.docker_catalog import DockerCatalog
from mslb_persistence.docker_records import ControlLease, DeploymentRecord, ResourceRecord
from mslb_persistence.docker_resources import DockerResources

from mslb_controller import inspection
from mslb_controller.backend_http import BackendHttp
from mslb_controller.engine import PROJECT, Engine
from mslb_controller.engine_models import ContainerInfo
from mslb_controller.errors import EngineError
from mslb_controller.routing_models import BackendSpec


class Lifecycle:
    """Reconcile trusted container identity before creating, starting or cleaning resources."""

    def __init__(self, database: Database, engine: Engine, http: BackendHttp) -> None:
        self.catalog = DockerCatalog(database=database)
        self.resources = DockerResources(database=database)
        self.engine, self.http = engine, http

    async def enroll(self) -> DeploymentRecord:
        image = await self.engine.image()
        net = await self.engine.network()
        first = await self.engine.inspect(reference=PROJECT + "-backend-a-1")
        second = await self.engine.inspect(reference=PROJECT + "-backend-b-1")
        return await self.catalog.enroll(
            definition=inspection.deployment(image=image, net=net, first=first, second=second)
        )

    async def _verify(self, record: ResourceRecord, info: ContainerInfo) -> None:
        """Inspect complete trusted identity before allowing a privileged lifecycle action."""
        definition = record.definition
        approved = (
            await self.catalog.deployment(deployment_id=definition.deployment_id)
        ).definition
        net = await self.engine.network()
        inspection.require(condition=net.Id == approved.network_id)
        image = await self.engine.image(image_id=approved.image_id)
        inspection.require(condition=image.Id == approved.image_id)
        if isinstance(definition, CreationIntent):
            inspection.dynamic(info=info, image=image, net=net, intent=definition)
        else:
            backend = inspection.static(info=info, image=image, net=net, name=record.name)
            inspection.require(
                condition=backend == definition.backend and backend in approved.static
            )
        if record.container_id is not None:
            inspection.require(condition=info.Id == record.container_id)

    async def observe(
        self, lease: ControlLease, identifier: UUID
    ) -> tuple[ResourceRecord, ContainerInfo]:
        """Reconcile the persisted exact resource with its actual daemon observation."""
        record = await self.resources.get(lease=lease, resource_id=identifier)
        if record.state in {"prepared", "removed", "abandoned", "released"}:
            raise EngineError(code="INVALID_STATE")
        reference = record.container_id or record.name
        info = await self.engine.inspect(reference=reference)
        try:
            await self._verify(record=record, info=info)
        except EngineError as error:
            # Only the container lookup above may supply an absence receipt. An image/network
            # 404 must never be confused with proof that the owned container disappeared.
            raise EngineError(code=error.code) from None
        # Recheck ownership after inspection, including cancellation/phase changes.
        await self.resources.get(lease=lease, resource_id=identifier)
        if isinstance(record.definition, CreationIntent):
            record = await self.resources.observe_dynamic(
                lease=lease,
                resource_id=identifier,
                observed=ContainerObservation(container_id=info.Id, definition=record.definition),
            )
            inspection.require(condition=record.state != "conflict")
        return record, info

    async def create(self, lease: ControlLease, identifier: UUID) -> ResourceRecord:
        """Reconcile a persisted deterministic intent before creating or adopting its container."""
        record = await self.resources.get(lease=lease, resource_id=identifier)
        inspection.require(condition=isinstance(record.definition, CreationIntent))
        # Revalidate the current deployment before granting new work. Old images remain usable
        # for cleanup, but a replaced static deployment must not silently authorize creation.
        enrolled = await self.enroll()
        inspection.require(condition=enrolled.id == record.definition.deployment_id)
        intent = await self.resources.authorize_create(lease=lease, resource_id=identifier)
        try:
            created = await self.engine.create(intent=intent)
        except EngineError as error:
            if error.status in {
                HTTPStatus.BAD_REQUEST.value,
                HTTPStatus.FORBIDDEN.value,
                HTTPStatus.NOT_FOUND.value,
                HTTPStatus.UNPROCESSABLE_CONTENT.value,
            }:
                await self.resources.reject_create(
                    lease=lease, resource_id=identifier, daemon_status=error.status
                )
            else:
                await self.resources.transition(
                    lease=lease, resource_id=identifier, action="uncertain"
                )
            raise
        info = await self.engine.inspect(reference=created.Id)
        await self._verify(record=record, info=info)
        record = await self.resources.observe_dynamic(
            lease=lease,
            resource_id=identifier,
            observed=ContainerObservation(container_id=info.Id, definition=intent),
        )
        if bool(created.Warnings):
            raise EngineError(code="ENGINE_CREATE_DIAGNOSTIC")
        return record

    async def start(self, lease: ControlLease, identifier: UUID) -> ResourceRecord:
        """Start only the inspected resource named by the current trusted reservation."""
        record, info = await self.observe(lease=lease, identifier=identifier)
        inspection.require(condition=isinstance(record.definition, CreationIntent))
        if record.state == "observed":
            await self.resources.transition(
                lease=lease, resource_id=identifier, action="authorize_start"
            )
            await self.engine.start(container_id=info.Id)
            record, info = await self.observe(lease=lease, identifier=identifier)
        # A lost start receipt may be observed, but never restart a stopped 'starting' resource.
        inspection.require(condition=info.State.get("Running") is True)
        if record.state == "starting":
            return await self.resources.transition(
                lease=lease, resource_id=identifier, action="started"
            )
        inspection.require(condition=record.state == "started" and lease.phase == "running")
        return record

    async def cleanup(self, lease: ControlLease, identifier: UUID) -> ResourceRecord:
        """Resolve owned resources while retaining uncertain intents for bounded reconciliation."""
        record = await self.resources.get(lease=lease, resource_id=identifier)
        if record.state in {"removed", "abandoned", "released"}:
            return record
        if record.state == "prepared":
            return await self.resources.transition(
                lease=lease, resource_id=identifier, action="abandon"
            )
        try:
            record, info = await self.observe(lease=lease, identifier=identifier)
        except EngineError as error:
            if error.status == HTTPStatus.NOT_FOUND.value and record.container_id is not None:
                return await self.resources.confirm_absent(
                    lease=lease, resource_id=identifier, container_id=record.container_id
                )
            # A name lookup returning 404 cannot prove a sent create will not arrive later.
            raise
        if record.state != "draining":
            record = await self.resources.transition(
                lease=lease, resource_id=identifier, action="drain"
            )
        if isinstance(record.definition, StaticReservation):
            health = await self.http.health(spec=BackendSpec(id=record.name))
            # Reinspect immutable identity after the HTTP observation; never stop a static backend.
            await self.observe(lease=lease, identifier=identifier)
            return await self.resources.release_static(
                lease=lease, resource_id=identifier, container_id=info.Id, observed=health
            )
        if info.State.get("Running") is True:
            await self.resources.get(lease=lease, resource_id=identifier)
            await self.engine.stop(container_id=info.Id)
        record, stopped = await self.observe(lease=lease, identifier=identifier)
        inspection.require(condition=stopped.State.get("Running") is False)
        await self.resources.get(lease=lease, resource_id=identifier)
        await self.engine.remove(container_id=info.Id)
        return await self.resources.transition(
            lease=lease, resource_id=identifier, action="removed"
        )

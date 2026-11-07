"""Provision and retire trusted backend membership through the restricted control client."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from mslb_contracts.docker_runtime import StopReason
from mslb_contracts.docker_settings import DockerSettings
from mslb_persistence.docker_records import ControlLease, ResourceRecord
from mslb_persistence.docker_resources import DockerResources
from mslb_persistence.errors import StoreError
from mslb_persistence.resource_evidence import ResourceEvidenceStore

from mslb_controller.backend_http import BackendHttp
from mslb_controller.control_client import ControlClient
from mslb_controller.errors import EngineError, GrowthHeld, TrafficError
from mslb_controller.physical_scaling import ReadyBackend
from mslb_controller.routing_models import BackendSpec


async def initial_backends(
    *, count: int, provision: Callable[[], Awaitable[ReadyBackend]], check: Callable[[], None]
) -> tuple[ReadyBackend, ...]:
    """Wait through soft growth holds within each initial backend's fifteen-second bound."""
    if type(count) is not int or not 1 <= count <= 4:
        raise ValueError("Invalid initial backend count")
    ready: list[ReadyBackend] = []
    for _ in range(count):
        async with asyncio.timeout(delay=15):
            while True:
                check()
                try:
                    ready.append(await provision())
                    break
                except GrowthHeld:
                    await asyncio.sleep(delay=0.1)
    return tuple(ready)


class ResourceObserver(Protocol):
    """Append only a trusted persisted resource receipt to operational evidence."""

    async def __call__(self, *, resource: ResourceRecord) -> None: ...


@dataclass(slots=True, kw_only=True)
class PhysicalResources:
    """Keep routing independent of daemon access and persist intents before every create."""

    resources: DockerResources
    client: ControlClient
    http: BackendHttp
    settings: DockerSettings
    lease: Callable[[], ControlLease]
    stop: Callable[[], StopReason | None]
    observe: ResourceObserver

    def check(self) -> None:
        """Raise TrafficError when trusted physical resource evidence is unsafe."""
        reason = self.stop()
        if reason is not None:
            raise TrafficError(code=reason)

    async def provision(self) -> ReadyBackend:
        """Reserve, inspect/create, start and health-check before routing membership exists."""
        self.check()
        record: ResourceRecord | None = None
        try:
            if self.resources.database.enforce_resource_guard:
                report = await ResourceEvidenceStore(database=self.resources.database).read()
                if not report.decision.admit:
                    if report.decision.stop_running:
                        raise TrafficError(code="RESOURCE_PRESSURE")
                    raise GrowthHeld
            async with asyncio.timeout(delay=15):
                records = await self.resources.list(lease=self.lease())
                occupied = {
                    record.name
                    for record in records
                    if record.state not in {"removed", "released", "abandoned"}
                }
                static = next(
                    (name for name in ("backend-a", "backend-b") if name not in occupied), None
                )
                record = (
                    await self.resources.reserve_static(lease=self.lease(), name=static)
                    if static is not None
                    else await self.resources.prepare_dynamic(lease=self.lease())
                )
                await self.observe(resource=record)
                self.check()
                if record.kind == "dynamic":
                    await self.client.resource(
                        lease=self.lease(), identifier=record.id, action="create"
                    )
                    record = await self.resources.get(lease=self.lease(), resource_id=record.id)
                    await self.observe(resource=record)
                    self.check()
                    await self.client.resource(
                        lease=self.lease(), identifier=record.id, action="start"
                    )
                else:
                    await self.client.resource(
                        lease=self.lease(), identifier=record.id, action="inspect"
                    )
                record = await self.resources.get(lease=self.lease(), resource_id=record.id)
                await self.observe(resource=record)
                spec = BackendSpec(
                    id=record.name,
                    resource_id=record.id,
                    container_id=record.container_id,
                    logical_admission_mem_mb=self.settings.logical_admission_mem_mb
                    if record.kind == "dynamic"
                    else 1024,
                    max_inflight=self.settings.backend_inflight if record.kind == "dynamic" else 4,
                    starts_per_second=self.settings.starts_per_backend_second,
                )
                for _ in range(20):
                    self.check()
                    try:
                        observed = await self.http.health(spec=spec)
                    except TrafficError:
                        observed = None
                    if observed is not None and observed.ready:
                        return ReadyBackend(spec=spec, observed=observed)
                    await asyncio.sleep(delay=0.1)
                raise TrafficError(code="INVALID_BACKEND")
        except (StoreError, EngineError) as failure:
            if not self.resources.database.enforce_resource_guard:
                raise
            report = await ResourceEvidenceStore(database=self.resources.database).read()
            pressure_refusal = (
                isinstance(failure, StoreError) and failure.code == "RESOURCE_PRESSURE"
            ) or (isinstance(failure, EngineError) and failure.code == "CONTROL_REFUSED")
            if pressure_refusal and not report.decision.admit and not report.decision.stop_running:
                if record is not None:
                    # Cleanup accepts held admission. If reconciliation is uncertain its
                    # failure propagates and capacity stays reserved for fenced recovery.
                    await self.client.resource(
                        lease=self.lease(), identifier=record.id, action="cleanup"
                    )
                    await self.observe(
                        resource=await self.resources.get(lease=self.lease(), resource_id=record.id)
                    )
                raise GrowthHeld from None
            raise

    async def retire(self, *, backend_id: str) -> None:
        """Resolve the exact persisted identity; static containers are never stopped."""
        records = await self.resources.list(lease=self.lease())
        record = next(
            (
                item
                for item in records
                if item.name == backend_id
                and item.state not in {"removed", "released", "abandoned"}
            ),
            None,
        )
        if record is None:
            raise TrafficError(code="INVALID_BACKEND")
        await self.client.resource(lease=self.lease(), identifier=record.id, action="cleanup")
        await self.observe(
            resource=await self.resources.get(lease=self.lease(), resource_id=record.id)
        )

    async def cleanup(self) -> None:
        """Attempt every unresolved owned resource once; retain uncertain capacity for recovery."""
        for record in await self.resources.list(lease=self.lease()):
            if record.state not in {"removed", "released", "abandoned"}:
                await self.retire(backend_id=record.name)

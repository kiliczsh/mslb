"""Fixed read-only resource measurements; no caller-selected daemon paths or services."""

import asyncio
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path

from mslb_contracts.resources import (
    MIB,
    SERVICE_LIMITS_MB,
    ResourceProbe,
    ServiceMemory,
    ServiceName,
)
from pydantic import ValidationError

from mslb_controller.engine import PROJECT, Engine
from mslb_controller.errors import EngineError
from mslb_controller.inspection import labels, mapping
from mslb_controller.probe_models import Capacity as Capacity
from mslb_controller.probe_models import InspectedMemory as InspectedMemory
from mslb_controller.probe_models import MemoryStats as MemoryStats


def integer(*, value: object, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise EngineError(code="RESOURCE_MEASUREMENTS")
    return value


def boolean(*, value: object) -> bool:
    if type(value) is not bool:
        raise EngineError(code="RESOURCE_MEASUREMENTS")
    return value


def available_memory() -> int:
    with Path("/proc/meminfo").open(mode="rb") as stream:
        raw = stream.read(64 * 1024 + 1)
    if len(raw) > 64 * 1024:
        raise EngineError(code="RESOURCE_MEASUREMENTS")
    for line in raw.splitlines():
        fields = line.split()
        if fields and fields[0] == b"MemAvailable:":
            if (
                len(fields) != 3
                or fields[2] != b"kB"
                or not fields[1].isdigit()
                or len(fields[1]) > 20
            ):
                raise EngineError(code="RESOURCE_MEASUREMENTS")
            return int(fields[1]) * 1024
    raise EngineError(code="RESOURCE_MEASUREMENTS")


class ProbeEngine(Engine):
    """Read only approved daemon capacity and service-memory measurements."""

    async def service(self, *, service: ServiceName) -> ServiceMemory:
        """Verify approved service identity before reading bounded daemon memory statistics."""
        if service not in SERVICE_LIMITS_MB:
            raise EngineError(code="INVALID_REFERENCE")
        name = f"{PROJECT}-{service}-1"
        try:
            info = await self._request(
                method="GET", path=f"/containers/{name}/json", model=InspectedMemory
            )
        except EngineError as error:
            if error.status != HTTPStatus.NOT_FOUND.value:
                raise
            return ServiceMemory(
                service=service,
                container_id=None,
                limit_bytes=SERVICE_LIMITS_MB[service] * MIB,
                usage_bytes=0,
                working_set_bytes=0,
                healthy=False,
                oom_killed=False,
                restart_count=0,
            )
        if info is None or info.Name != "/" + name:
            raise EngineError(code="RESOURCE_MEASUREMENTS")
        identity = labels(value=info.Config.get("Labels"))
        if (
            identity.get("com.docker.compose.project") != PROJECT
            or identity.get("com.docker.compose.service") != service
            or identity.get("com.docker.compose.oneoff") != "False"
        ):
            raise EngineError(code="RESOURCE_MEASUREMENTS")
        limit = integer(value=info.HostConfig.get("Memory"), minimum=1)
        running = boolean(value=info.State.get("Running"))
        healthy = running and mapping(value=info.State.get("Health", {})).get("Status") == "healthy"
        usage = working = 0
        if running:
            stats = await self._request(
                method="GET",
                path=f"/containers/{info.Id}/stats?stream=false&one-shot=true",
                model=MemoryStats,
            )
            if stats is None or integer(value=stats.memory_stats.get("limit"), minimum=1) != limit:
                raise EngineError(code="RESOURCE_MEASUREMENTS")
            usage = integer(value=stats.memory_stats.get("usage"))
            counters = mapping(value=stats.memory_stats.get("stats", {}))
            inactive = integer(
                value=counters.get("inactive_file", counters.get("total_inactive_file", 0))
            )
            if inactive > usage:
                raise EngineError(code="RESOURCE_MEASUREMENTS")
            working = usage - inactive
        return ServiceMemory(
            service=service,
            container_id=info.Id,
            limit_bytes=limit,
            usage_bytes=usage,
            working_set_bytes=working,
            healthy=healthy,
            oom_killed=boolean(value=info.State.get("OOMKilled")),
            restart_count=info.RestartCount,
        )

    async def probe(self) -> ResourceProbe:
        """Collect approved service/engine measurements and optional host evidence."""
        gate = asyncio.Semaphore(value=2)

        async def sample(*, service: ServiceName) -> ServiceMemory:
            async with gate:
                return await self.service(service=service)

        try:
            async with asyncio.timeout(delay=3):
                capacity = await self._request(method="GET", path="/info", model=Capacity)
                if capacity is None or capacity.OSType != "linux":
                    raise EngineError(code="RESOURCE_MEASUREMENTS")
                results = await asyncio.gather(
                    *(sample(service=service) for service in SERVICE_LIMITS_MB),
                    return_exceptions=True,
                )
                measured: list[ServiceMemory] = []
                for result in results:
                    if not isinstance(result, ServiceMemory):
                        raise EngineError(code="RESOURCE_MEASUREMENTS")
                    measured.append(result)
                available = await asyncio.to_thread(available_memory)
                return ResourceProbe(
                    measured_at=datetime.now(tz=UTC),
                    engine_memory_bytes=capacity.MemTotal,
                    available_memory_bytes=available,
                    desktop_engine="Docker Desktop" in capacity.OperatingSystem,
                    services=tuple(measured),
                )
        except (OSError, TimeoutError, ValidationError):
            raise EngineError(code="RESOURCE_MEASUREMENTS") from None

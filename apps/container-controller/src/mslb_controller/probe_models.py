"""Bounded daemon resource-probe response DTOs."""

from typing import Annotated

from pydantic import Field

from mslb_controller.engine_models import ContainerInfo, EngineModel


class Capacity(EngineModel):
    """Decode the engine memory capacity used for admission calculations."""

    MemTotal: Annotated[int, Field(ge=1)]
    OSType: str
    OperatingSystem: str


class InspectedMemory(ContainerInfo):
    """Decode bounded container health, restart and memory-limit evidence."""

    RestartCount: Annotated[int, Field(ge=0)]


class MemoryStats(EngineModel):
    """Decode daemon memory counters used to calculate working-set usage."""

    memory_stats: dict[str, object]

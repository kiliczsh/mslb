"""Bounded daemon resource-probe response DTOs."""

from typing import Annotated

from pydantic import Field

from mslb_controller.engine_models import ContainerInfo, EngineModel


class Capacity(EngineModel):
    MemTotal: Annotated[int, Field(ge=1)]
    OSType: str
    OperatingSystem: str


class InspectedMemory(ContainerInfo):
    RestartCount: Annotated[int, Field(ge=0)]


class MemoryStats(EngineModel):
    memory_stats: dict[str, object]

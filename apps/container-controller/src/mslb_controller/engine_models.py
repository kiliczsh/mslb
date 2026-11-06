"""Bounded daemon inspection and creation response models."""

from typing import Annotated

from mslb_contracts.docker_base import DockerId, ImageId
from pydantic import BaseModel, ConfigDict, Field


class EngineModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")


class ImageInfo(EngineModel):
    Id: ImageId
    Config: dict[str, object]


class NetworkInfo(EngineModel):
    Id: DockerId
    Name: Annotated[str, Field(max_length=128)]
    Driver: Annotated[str, Field(max_length=64)]
    Internal: bool
    Scope: Annotated[str, Field(max_length=32)]
    Labels: dict[str, str]


class ContainerInfo(EngineModel):
    Id: DockerId
    Image: ImageId
    Name: Annotated[str, Field(max_length=128)]
    Config: dict[str, object]
    HostConfig: dict[str, object]
    State: dict[str, object]
    NetworkSettings: dict[str, object]
    Mounts: Annotated[list[dict[str, object]], Field(max_length=8)]


class Created(EngineModel):
    Id: DockerId
    Warnings: Annotated[list[str], Field(max_length=16)] | None = None

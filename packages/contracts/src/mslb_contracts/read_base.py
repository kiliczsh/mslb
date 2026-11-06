"""Strict transport-neutral base and shared read value types."""

from typing import Literal

from pydantic import BaseModel, ConfigDict

type JsonValue = None | bool | int | str | list[JsonValue] | dict[str, JsonValue]


type JobStatus = Literal[
    "queued", "running", "cancel_requested", "completed", "failed", "cancelled"
]


class ReadModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

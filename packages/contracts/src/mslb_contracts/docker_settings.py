"""Bounded real-time traffic and physical scaling configuration."""

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from mslb_contracts.docker_base import DockerModel
from mslb_contracts.inputs import PositiveValue


class PhysicalScaling(DockerModel):
    enabled: bool = False
    evaluation_seconds: Annotated[int, Field(ge=1, le=1)] = 1
    pending_samples: Annotated[int, Field(ge=2, le=10)] = 2
    idle_samples: Annotated[int, Field(ge=5, le=30)] = 5
    cooldown_seconds: Annotated[int, Field(ge=3, le=30)] = 3


class DockerSettings(DockerModel):
    mode: Literal["docker"] = "docker"
    strategy: Literal["first_fit", "round_robin", "least_connections", "most_available_memory"] = (
        "first_fit"
    )
    backend_template: Literal["bounded-python-work"] = "bounded-python-work"
    initial_replicas: Annotated[int, Field(ge=1, le=4)] = 2
    min_replicas: Annotated[int, Field(ge=1, le=4)] = 1
    max_replicas: Annotated[int, Field(ge=1, le=4)] = 4
    global_inflight: Annotated[int, Field(ge=1, le=16)] = 16
    backend_inflight: Annotated[int, Field(ge=1, le=4)] = 4
    logical_admission_mem_mb: PositiveValue = 1024
    starts_per_backend_second: Annotated[int, Field(ge=1, le=32)] = 4
    max_pending: Annotated[int, Field(ge=1, le=256)] = 64
    pending_timeout_seconds: Annotated[int, Field(ge=1, le=120)] = 5
    duration_seconds: Annotated[int, Field(ge=1, le=120)] = 120
    request_timeout_ms: Annotated[int, Field(ge=100, le=3000)] = 3000
    health_timeout_ms: Annotated[int, Field(ge=100, le=1000)] = 1000
    drain_timeout_seconds: Annotated[int, Field(ge=1, le=5)] = 3
    backend_memory_mb: Annotated[int, Field(ge=96, le=128)] = 128
    backend_nano_cpus: Annotated[int, Field(ge=125_000_000, le=500_000_000)] = 500_000_000
    physical_scaling: PhysicalScaling = Field(default_factory=PhysicalScaling)

    @model_validator(mode="after")
    def bounds(self) -> Self:
        if not self.min_replicas <= self.initial_replicas <= self.max_replicas:
            raise ValueError("Replica bounds conflict")
        return self

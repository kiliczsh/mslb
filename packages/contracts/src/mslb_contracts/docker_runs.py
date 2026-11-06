"""Operational Docker provenance and lifecycle contracts; never logical replay inputs."""

import hashlib
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.durability import StoredFile
from mslb_contracts.inputs import PositiveValue
from mslb_contracts.runs import Digest

type DockerId = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
type ImageId = Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]


class DockerModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    def digest(self) -> str:
        return hashlib.sha256(canonical_bytes(self.model_dump(mode="json"))).hexdigest()


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


class StaticBackend(DockerModel):
    name: Literal["backend-a", "backend-b"]
    container_id: DockerId
    logical_admission_mem_mb: PositiveValue = 1024
    max_inflight: Annotated[int, Field(ge=4, le=4)] = 4


class DeploymentSpec(DockerModel):
    """Server-enrolled inspected identity, not a public run-creation DTO."""

    project: Literal["mslb"] = "mslb"
    image_id: ImageId
    network_id: DockerId
    static: Annotated[tuple[StaticBackend, ...], Field(min_length=2, max_length=2)]

    @model_validator(mode="after")
    def members(self) -> Self:
        if (
            tuple(item.name for item in self.static) != ("backend-a", "backend-b")
            or len({item.container_id for item in self.static}) != 2
        ):
            raise ValueError("Static inventory must contain the two distinct ordered backends")
        return self


class DockerSnapshot(DockerModel):
    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    controller_version: Literal["0.1.0"] = "0.1.0"
    dataset_id: UUID
    dataset_digest: Digest
    request_count: Annotated[int, Field(ge=1, le=10_000)]
    latest_arrival: Annotated[int, Field(ge=0, le=119)]
    deployment_id: UUID
    deployment_digest: Digest
    configuration: DockerSettings

    @model_validator(mode="after")
    def arrival(self) -> Self:
        if self.latest_arrival >= self.configuration.duration_seconds:
            raise ValueError("Dataset arrivals exceed the real-time run duration")
        return self


class CreationIntent(DockerModel):
    """Complete server-derived creation definition, frozen before daemon invocation."""

    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    deployment_id: UUID
    run_id: UUID
    intent_id: UUID
    creator_fence: Annotated[int, Field(ge=1)]
    slot: Annotated[int, Field(ge=0, le=3)]
    image_id: ImageId
    network_id: DockerId
    logical_admission_mem_mb: PositiveValue
    max_inflight: Annotated[int, Field(ge=1, le=4)]
    memory_mb: Annotated[int, Field(ge=96, le=128)]
    nano_cpus: Annotated[int, Field(ge=125_000_000, le=500_000_000)]

    @property
    def name(self) -> str:
        return f"mslb-{self.intent_id.hex}-{self.slot}"

    def daemon_body(self) -> dict[str, object]:
        """Render a fixed allowlist; privileged fields cannot come from caller JSON."""
        return {
            "Image": self.image_id,
            "Entrypoint": ["python", "-m", "mslb_mock_server"],
            "Cmd": [],
            "User": "1000:1000",
            "WorkingDir": "/app",
            "Env": [
                f"BACKEND_ID={self.name}",
                f"LOGICAL_ADMISSION_MEM_MB={self.logical_admission_mem_mb}",
                f"MAX_INFLIGHT={self.max_inflight}",
                "TMPDIR=/run/mslb-tmp",
            ],
            "Labels": {
                "com.mslb.component": "http-backend",
                "com.mslb.backend-kind": "dynamic",
                "com.mslb.deployment": str(self.deployment_id),
                "com.mslb.run": str(self.run_id),
                "com.mslb.intent": str(self.intent_id),
                "com.mslb.fence": str(self.creator_fence),
                "com.mslb.definition": self.digest(),
            },
            "HostConfig": {
                "Memory": self.memory_mb * 1024**2,
                "MemorySwap": self.memory_mb * 1024**2,
                "NanoCpus": self.nano_cpus,
                "PidsLimit": 32,
                "ReadonlyRootfs": True,
                "CapDrop": ["ALL"],
                "SecurityOpt": ["no-new-privileges:true"],
                "NetworkMode": self.network_id,
                "PublishAllPorts": False,
                "PortBindings": {},
                "Tmpfs": {
                    "/run/mslb-tmp": "rw,noexec,nosuid,nodev,size=16777216,uid=1000,gid=1000"
                },
                "LogConfig": {
                    "Type": "json-file",
                    "Config": {
                        "max-size": "5m",
                        "max-file": "2",
                        "mode": "non-blocking",
                        "max-buffer-size": "1m",
                    },
                },
            },
            "NetworkingConfig": {"EndpointsConfig": {self.network_id: {"Aliases": [self.name]}}},
        }


class ContainerObservation(DockerModel):
    """Trusted inspect adapter output; matching a label alone is insufficient."""

    container_id: DockerId
    definition: CreationIntent


class StaticReservation(DockerModel):
    deployment_id: UUID
    run_id: UUID
    resource_id: UUID
    slot: Annotated[int, Field(ge=0, le=3)]
    backend: StaticBackend


class DockerManifest(DockerModel):
    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    mode: Literal["docker"] = "docker"
    run_id: UUID
    publication_fence: Annotated[int, Field(ge=1)]
    snapshot_digest: Digest
    total: Annotated[int, Field(ge=1, le=10_000)]
    completed: Annotated[int, Field(ge=0, le=10_000)]
    failed: Annotated[int, Field(ge=0, le=10_000)]
    dropped: Annotated[int, Field(ge=0, le=10_000)]
    unexecuted: Annotated[int, Field(ge=0, le=10_000)]
    artifacts: Annotated[dict[str, StoredFile], Field(min_length=3, max_length=3)]

    @model_validator(mode="after")
    def artifacts_and_accounting(self) -> Self:
        names = {"http_events.jsonl", "runtime_summary.json", "container_lifecycle.jsonl"}
        prefix = f"docker/{self.run_id.hex}/{self.publication_fence}/"
        if (
            self.completed + self.failed + self.dropped + self.unexecuted != self.total
            or set(self.artifacts) != names
            or sum(value.size_bytes for value in self.artifacts.values()) > 512 * 1024**2
            or any(value.key != prefix + name for name, value in self.artifacts.items())
        ):
            raise ValueError("Docker artifact namespace or terminal accounting is invalid")
        return self

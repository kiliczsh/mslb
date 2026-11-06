"""Frozen daemon creation intents and inspected ownership receipts."""

from typing import Annotated
from uuid import UUID

from pydantic import Field

from mslb_contracts.docker_base import DockerId, DockerModel, ImageId
from mslb_contracts.docker_inventory import StaticBackend
from mslb_contracts.inputs import PositiveValue


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
                "com.mslb.deployment": str(object=self.deployment_id),
                "com.mslb.run": str(object=self.run_id),
                "com.mslb.intent": str(object=self.intent_id),
                "com.mslb.fence": str(object=self.creator_fence),
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
    """Bind a static backend exclusively to a durable run resource slot."""

    deployment_id: UUID
    run_id: UUID
    resource_id: UUID
    slot: Annotated[int, Field(ge=0, le=3)]
    backend: StaticBackend

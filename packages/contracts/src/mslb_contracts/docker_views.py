"""Public operational views omit daemon commands, credentials and managed storage paths."""

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field

from mslb_contracts.docker_runtime import TrafficSummary
from mslb_contracts.docker_settings import DockerSettings
from mslb_contracts.http import SimulationCreate
from mslb_contracts.read_base import JobStatus, ReadModel
from mslb_contracts.resources import ResourceDecision
from mslb_contracts.runs import Digest

type DockerArtifactName = Literal[
    "http_events.jsonl",
    "container_lifecycle.jsonl",
    "runtime_summary.json",
    "snapshot.json",
    "requests.csv",
]


class DockerSimulationCreate(DockerSettings):
    """Submit registered bounded input with trusted server-selected deployment identity."""

    mode: Literal["docker"] = Field(default=...)
    dataset_id: Annotated[UUID, Field(strict=False)]


class DockerRunCommand(ReadModel):
    """Submit operational work with a bounded idempotency key through MCP."""

    simulation: DockerSimulationCreate
    idempotency_key: Annotated[str, Field(min_length=1, max_length=128)] | None = None


class DockerEventArguments(ReadModel):
    """Page actual HTTP observations without introducing logical tick parameters."""

    run_id: UUID
    limit: Annotated[int, Field(ge=1, le=500)] = 100
    cursor: Annotated[str, Field(min_length=1, max_length=4096)] | None = None


type SimulationRequest = SimulationCreate | DockerSimulationCreate


class DockerRunView(ReadModel):
    """Read frozen operational settings and durable status independently of logical ticks."""

    mode: Literal["docker"] = "docker"
    id: UUID
    dataset_id: UUID
    snapshot_digest: Digest
    dataset_digest: Digest
    configuration: DockerSettings
    status: JobStatus
    attempts: Annotated[int, Field(ge=0, le=3)]
    created_at: datetime
    completed_at: datetime | None
    error_code: str | None


class DockerResourceView(ReadModel):
    """Expose scoped lifecycle identity without privileged creation definitions."""

    id: UUID
    name: Annotated[str, Field(min_length=1, max_length=128)]
    kind: Literal["static", "dynamic"]
    state: Literal[
        "prepared",
        "creating",
        "observed",
        "starting",
        "started",
        "draining",
        "removed",
        "released",
        "abandoned",
        "conflict",
        "unresolved",
    ]


class DockerArtifactView(ReadModel):
    """Identify a typed verified download without disclosing its internal file key."""

    name: DockerArtifactName
    sha256: Digest
    size_bytes: Annotated[int, Field(ge=0, le=256 * 1024**2)]


class DockerRunDetail(ReadModel):
    """Keep actual HTTP outcomes, owned lifecycle and current admission explicitly separate."""

    run: DockerRunView
    cleanup_pending: bool
    resources: Annotated[tuple[DockerResourceView, ...], Field(max_length=64)]
    summary: TrafficSummary | None = None
    artifacts: Annotated[tuple[DockerArtifactView, ...], Field(max_length=5)] = ()
    admission: ResourceDecision | None = None

"""Immutable repository results; callers never receive live ORM entities."""

from dataclasses import dataclass
from datetime import datetime
from typing import Self
from uuid import UUID

from mslb_contracts.catalog import ServerView
from mslb_contracts.inputs import ServerInput

from mslb_persistence.catalog_models import Dataset, ManagedServer
from mslb_persistence.job_models import Job, Run


def server_view(server: ManagedServer) -> ServerView:
    """Map a current server row into its versioned public representation."""
    return ServerView(
        id=server.id,
        version=server.version,
        cpu_units_per_tick=server.cpu_units_per_tick,
        mem_mb=server.mem_mb,
        rate_limit_per_sec=server.rate_limit_per_sec,
    )


def server_input(server: ManagedServer) -> ServerInput:
    """Freeze a database server row into a simulation input value."""
    return ServerInput(
        id=server.id,
        cpu_units_per_tick=server.cpu_units_per_tick,
        mem_mb=server.mem_mb,
        rate_limit_per_sec=server.rate_limit_per_sec,
    )


@dataclass(frozen=True, slots=True)
class DatasetRecord:
    """Return immutable dataset metadata without exposing a live database entity."""

    id: UUID
    digest: str
    storage_key: str
    source_filename: str
    request_count: int
    earliest_arrival: int
    latest_arrival: int
    size_bytes: int
    created_at: datetime
    original_digest: str | None
    source_size_bytes: int | None

    @classmethod
    def from_model(cls, row: Dataset) -> Self:
        """Detach dataset metadata from a database ORM row."""
        return cls(
            id=row.id,
            digest=row.digest,
            storage_key=row.storage_key,
            source_filename=row.source_filename,
            request_count=row.request_count,
            earliest_arrival=row.earliest_arrival,
            latest_arrival=row.latest_arrival,
            size_bytes=row.size_bytes,
            created_at=row.created_at,
            original_digest=row.original_digest,
            source_size_bytes=row.source_size_bytes,
        )


@dataclass(frozen=True, slots=True)
class RunRecord:
    """Return frozen run identities and configuration for application execution."""

    id: UUID
    snapshot_digest: str
    snapshot_key: str
    dataset_id: UUID
    server_versions: tuple[tuple[str, int], ...]
    created_at: datetime

    @classmethod
    def from_model(cls, row: Run) -> Self:
        """Detach immutable snapshot identity, server versions and creation metadata."""
        return cls(
            id=row.id,
            snapshot_digest=row.snapshot_digest,
            snapshot_key=row.snapshot_key,
            dataset_id=row.dataset_id,
            server_versions=tuple(sorted(row.server_versions.items())),
            created_at=row.created_at,
        )


@dataclass(frozen=True, slots=True)
class Lease:
    """Carry the logical-job owner, attempt, expiry and fencing token."""

    run_id: UUID
    attempt: int
    fence: int
    owner: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class JobRecord:
    """Return a durable job state and safe operational error code."""

    run_id: UUID
    status: str
    attempts: int
    fence: int
    error_code: str | None
    lease_expires_at: datetime | None

    @classmethod
    def from_model(cls, row: Job) -> Self:
        """Detach job status, attempt/fence counters and lease expiry from an ORM row."""
        return cls(
            run_id=row.run_id,
            status=row.status,
            attempts=row.attempts,
            fence=row.fence,
            error_code=row.error_code,
            lease_expires_at=row.lease_expires_at,
        )

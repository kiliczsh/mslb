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
    return ServerView(
        id=server.id,
        version=server.version,
        cpu_units_per_tick=server.cpu_units_per_tick,
        mem_mb=server.mem_mb,
        rate_limit_per_sec=server.rate_limit_per_sec,
    )


def server_input(server: ManagedServer) -> ServerInput:
    return ServerInput(
        id=server.id,
        cpu_units_per_tick=server.cpu_units_per_tick,
        mem_mb=server.mem_mb,
        rate_limit_per_sec=server.rate_limit_per_sec,
    )


@dataclass(frozen=True, slots=True)
class DatasetRecord:
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
        return cls(
            row.id,
            row.digest,
            row.storage_key,
            row.source_filename,
            row.request_count,
            row.earliest_arrival,
            row.latest_arrival,
            row.size_bytes,
            row.created_at,
            row.original_digest,
            row.source_size_bytes,
        )


@dataclass(frozen=True, slots=True)
class RunRecord:
    id: UUID
    snapshot_digest: str
    snapshot_key: str
    dataset_id: UUID
    server_versions: tuple[tuple[str, int], ...]
    created_at: datetime

    @classmethod
    def from_model(cls, row: Run) -> Self:
        return cls(
            row.id,
            row.snapshot_digest,
            row.snapshot_key,
            row.dataset_id,
            tuple(sorted(row.server_versions.items())),
            row.created_at,
        )


@dataclass(frozen=True, slots=True)
class Lease:
    run_id: UUID
    attempt: int
    fence: int
    owner: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class JobRecord:
    run_id: UUID
    status: str
    attempts: int
    fence: int
    error_code: str | None
    lease_expires_at: datetime | None

    @classmethod
    def from_model(cls, row: Job) -> Self:
        return cls(
            row.run_id, row.status, row.attempts, row.fence, row.error_code, row.lease_expires_at
        )

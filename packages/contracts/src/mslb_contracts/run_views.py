"""Run status, progress, validation receipts and bounded wait results."""

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field

from mslb_contracts.capacity_profiles import MAX_LOGICAL_EVENTS
from mslb_contracts.read_base import JobStatus, ReadModel
from mslb_contracts.runs import Digest, ReadRunSettings


class RunProgress(ReadModel):
    """Expose only committed interval or terminal progress and event counts."""

    revision: Annotated[int, Field(ge=1)]
    kind: Literal["checkpoint", "terminal"]
    next_tick: Annotated[int, Field(ge=0, le=1_000_000)]
    executed_intervals: Annotated[int, Field(ge=0, le=1_000_000)]
    committed_events: Annotated[int, Field(ge=0, le=MAX_LOGICAL_EVENTS)]


class RunView(ReadModel):
    """Expose coherent durable status and frozen configuration without internal storage paths."""

    id: UUID
    dataset_id: UUID
    snapshot_digest: Digest
    dataset_digest: Digest
    server_snapshot_digest: Digest
    engine_version: Annotated[str, Field(min_length=1, max_length=64)]
    configuration: ReadRunSettings
    status: JobStatus
    attempts: Annotated[int, Field(ge=0, le=3)]
    created_at: datetime
    completed_at: datetime | None
    error_code: Annotated[str, Field(max_length=64)] | None
    progress: RunProgress | None


class PublishedValidation(ReadModel):
    """Receipt from the verified, durably published manifest; no storage paths."""

    bundle_digest: Digest
    independent_replay: Literal["passed"]
    original_validator: Literal["passed", "not_applicable"]


class RunStatus(ReadModel):
    """Return compact durable progress and published validation for bounded waits."""

    run_id: UUID
    status: JobStatus
    attempts: Annotated[int, Field(ge=0, le=3)]
    error_code: Annotated[str, Field(max_length=64)] | None
    progress: RunProgress | None
    validation: PublishedValidation | None


class WaitResult(ReadModel):
    """Return a bounded status batch with explicit terminal and timeout outcomes."""

    statuses: Annotated[tuple[RunStatus, ...], Field(min_length=1, max_length=8)]
    all_terminal: bool
    timed_out: bool

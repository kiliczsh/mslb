"""Compact filtered history projections shared by the two browser workspaces."""

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field

from mslb_contracts.capacity_profiles import STRESS_REQUESTS
from mslb_contracts.read_base import JobStatus, ReadModel
from mslb_contracts.run_views import RunProgress

type HistoryKind = Literal["logical", "docker"]
type HistoryStatus = Literal[
    "active", "queued", "running", "cancel_requested", "completed", "failed", "cancelled"
]
type HistoryStrategy = Literal[
    "first_fit", "round_robin", "least_connections", "most_available_memory"
]
type HistoryMode = Literal["sequential", "concurrent", "docker"]


class HistoryFilters(ReadModel):
    """Bind bounded search and exact filters into signed continuation cursors."""

    search: Annotated[str, Field(max_length=128)] = ""
    status: HistoryStatus | None = None
    strategy: HistoryStrategy | None = None
    mode: HistoryMode | None = None


class HistoryEntry(ReadModel):
    """Identify experiments without fetching datasets or result files per row."""

    id: UUID
    dataset_id: UUID
    source_filename: Annotated[str, Field(min_length=1, max_length=255)]
    request_count: Annotated[int, Field(ge=1, le=STRESS_REQUESTS)]
    strategy: HistoryStrategy
    execution_mode: HistoryMode
    status: JobStatus
    created_at: datetime
    completed_at: datetime | None
    error_code: Annotated[str, Field(max_length=64)] | None
    progress: RunProgress | None

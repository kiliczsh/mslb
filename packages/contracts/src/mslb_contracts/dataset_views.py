"""Immutable dataset metadata without storage paths."""

from datetime import datetime
from typing import Annotated
from uuid import UUID

from pydantic import Field

from mslb_contracts.capacity_profiles import STRESS_REQUESTS
from mslb_contracts.inputs import RequestInput
from mslb_contracts.read_base import ReadModel
from mslb_contracts.runs import Digest


class DatasetView(ReadModel):
    """Expose immutable dataset identity, counts and upload provenance without storage keys."""

    id: UUID
    digest: Digest
    source_filename: Annotated[str, Field(min_length=1, max_length=255)]
    request_count: Annotated[int, Field(ge=1, le=STRESS_REQUESTS)]
    earliest_arrival: Annotated[int, Field(ge=0, le=999_999)]
    latest_arrival: Annotated[int, Field(ge=0, le=999_999)]
    size_bytes: Annotated[int, Field(ge=1, le=10 * 1024**2)]
    created_at: datetime
    original_digest: Digest | None
    source_size_bytes: Annotated[int, Field(ge=1, le=10 * 1024**2)] | None


class DatasetPreview(ReadModel):
    """Expose at most twenty verified normalized CSV records."""

    dataset_id: UUID
    total_rows: Annotated[int, Field(ge=1, le=STRESS_REQUESTS)]
    rows: Annotated[tuple[RequestInput, ...], Field(min_length=1, max_length=20)]

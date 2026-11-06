"""Versioned configuration and immutable dataset-registration metadata."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from mslb_contracts.inputs import ServerInput
from mslb_contracts.runs import Digest


class ServerView(ServerInput):
    """Expose editable server capacities and the version required for future mutations."""

    version: Annotated[int, Field(ge=1)]


class DatasetRegistration(BaseModel):
    """Register validated normalized input with exact byte and source provenance."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    digest: Digest
    source_filename: Annotated[str, Field(min_length=1, max_length=255)]
    request_count: Annotated[int, Field(ge=1, le=10_000)]
    earliest_arrival: Annotated[int, Field(ge=0, le=999_999)]
    latest_arrival: Annotated[int, Field(ge=0, le=999_999)]
    size_bytes: Annotated[int, Field(ge=1, le=10 * 1024 * 1024)]
    original_digest: Digest | None = None
    source_size_bytes: Annotated[int, Field(ge=1, le=10 * 1024 * 1024)] | None = None

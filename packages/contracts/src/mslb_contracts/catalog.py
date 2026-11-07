"""Versioned configuration and immutable dataset-registration metadata."""

from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from mslb_contracts.capacity_profiles import STRESS_REQUESTS, CapacityProfile, request_limit
from mslb_contracts.inputs import ServerInput
from mslb_contracts.runs import Digest


class ServerView(ServerInput):
    """Expose editable server capacities and the version required for future mutations."""

    version: Annotated[int, Field(ge=1)]


class ServerConfiguration(BaseModel):
    """Replace a small cluster only if the complete reviewed catalog still matches."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    expected: Annotated[list[ServerView], Field(max_length=64)]
    servers: Annotated[list[ServerInput], Field(min_length=3, max_length=5)]

    @model_validator(mode="after")
    def unique_servers(self) -> Self:
        """Reject duplicate identities in either catalog before any mutation."""
        if len({server.id for server in self.expected}) != len(self.expected) or len(
            {server.id for server in self.servers}
        ) != len(self.servers):
            raise ValueError("Server IDs must be unique")
        return self


class DatasetRegistration(BaseModel):
    """Register validated normalized input with exact byte and source provenance."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    digest: Digest
    source_filename: Annotated[str, Field(min_length=1, max_length=255)]
    request_count: Annotated[int, Field(ge=1, le=STRESS_REQUESTS)]
    earliest_arrival: Annotated[int, Field(ge=0, le=999_999)]
    latest_arrival: Annotated[int, Field(ge=0, le=999_999)]
    size_bytes: Annotated[int, Field(ge=1, le=10 * 1024 * 1024)]
    original_digest: Digest | None = None
    source_size_bytes: Annotated[int, Field(ge=1, le=10 * 1024 * 1024)] | None = None
    capacity_profile: CapacityProfile = "standard"

    @model_validator(mode="after")
    def population(self) -> Self:
        """Bind registration population to the server-selected ingestion profile."""
        if self.request_count > request_limit(profile=self.capacity_profile):
            raise ValueError("Dataset population exceeds its capacity profile")
        return self

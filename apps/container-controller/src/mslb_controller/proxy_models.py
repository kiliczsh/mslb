"""Strict semantic proxy request DTOs with database-authoritative leases."""

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from mslb_persistence.docker_records import ControlLease
from pydantic import BaseModel, ConfigDict, Field


class LeaseRef(BaseModel):
    """Accept a controller identity while reading authoritative expiry from PostgreSQL."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    run_id: UUID
    fence: Annotated[int, Field(ge=1, le=9223372036854775807)]
    owner: Annotated[str, Field(min_length=1, max_length=128)]
    phase: Literal["running", "cleanup"]

    def lease(self) -> ControlLease:
        # Expiry is read and enforced in PostgreSQL, never accepted from the caller.
        return ControlLease(
            run_id=self.run_id,
            fence=self.fence,
            owner=self.owner,
            expires_at=datetime.min.replace(tzinfo=UTC),
            phase=self.phase,
        )


class Empty(BaseModel):
    """Require an empty strict body for semantic proxy operations without parameters."""

    model_config = ConfigDict(strict=True, extra="forbid")

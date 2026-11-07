"""Closed server-owned logical workload profiles; clients cannot select higher ceilings."""

from typing import Literal
from uuid import UUID

type CapacityProfile = Literal["standard", "stress"]

STANDARD_REQUESTS = 10_000
STRESS_REQUESTS = 50_000
MAX_LOGICAL_EVENTS = 3 * STRESS_REQUESTS


def parse_capacity_profile(*, value: str) -> CapacityProfile:
    """Validate an installation setting without echoing an untrusted environment value."""
    if value == "standard":
        return "standard"
    if value == "stress":
        return "stress"
    raise ValueError("Unsupported installation capacity profile")


def request_limit(*, profile: CapacityProfile) -> int:
    """Return the finite request ceiling for a trusted installation profile."""
    return (
        STRESS_REQUESTS if parse_capacity_profile(value=profile) == "stress" else STANDARD_REQUESTS
    )


def validate_request_limit(*, value: int) -> None:
    """Accept only the two versioned logical population bounds."""
    if type(value) is not int or value not in {STANDARD_REQUESTS, STRESS_REQUESTS}:
        raise ValueError("Unsupported logical request capacity")


def validate_isolated_profile(*, profile: CapacityProfile, installation_id: UUID | None) -> None:
    """Require explicit isolation identity before a service enables the stress envelope."""
    request_limit(profile=profile)
    if profile == "stress" and not isinstance(installation_id, UUID):
        raise ValueError("Stress capacity requires an isolated installation identity")

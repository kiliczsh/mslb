"""Backend inventory, routing state and idempotent admission reservations."""

import re
from dataclasses import dataclass, field
from typing import Literal, Protocol
from uuid import UUID

from mslb_contracts.traffic import BackendHealth, WorkRequest

type Strategy = Literal["first_fit", "round_robin", "least_connections", "most_available_memory"]
STRATEGIES: tuple[Strategy, ...] = (
    "first_fit",
    "round_robin",
    "least_connections",
    "most_available_memory",
)


@dataclass(frozen=True, slots=True, kw_only=True)
class BackendSpec:
    """Trusted lifecycle inventory, never a public caller's URL/image configuration."""

    id: str
    logical_admission_mem_mb: int = 1024
    max_inflight: int = 4
    starts_per_second: int = 4
    resource_id: UUID | None = field(default=None, kw_only=True)
    container_id: str | None = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        if not re.fullmatch(pattern=r"backend-[ab]|mslb-[0-9a-f]{32}-[0-3]", string=self.id):
            raise ValueError("Only dedicated backend network identities are permitted")
        for value, maximum in (
            (self.logical_admission_mem_mb, 1_000_000_000),
            (self.max_inflight, 4),
            (self.starts_per_second, 32),
        ):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError("Backend admission configuration exceeds its envelope")

        if (self.resource_id is None) != (self.container_id is None) or (
            self.container_id is not None
            and re.fullmatch(pattern=r"[0-9a-f]{64}", string=self.container_id) is None
        ):
            raise ValueError("Backend ownership identifiers must match")

    @property
    def url(self) -> str:
        return f"http://{self.id}:8080"


class HealthObserver(Protocol):
    """Observe only an actual health result or explicit unavailable response."""

    def __call__(self, *, spec: BackendSpec, health: BackendHealth | None) -> None: ...


@dataclass(slots=True)
class BackendState:
    """Track live eligibility and logical reservations for one HTTP backend."""

    spec: BackendSpec
    healthy: bool = False
    draining: bool = False
    inflight: int = 0
    reserved_mem_mb: int = 0
    start_bucket: int = -1
    starts: int = 0
    observed: BackendHealth | None = None


@dataclass(slots=True)
class Reservation:
    """An event-loop-owned reservation; release is idempotent across all terminal paths."""

    backend: BackendState
    request: WorkRequest
    released: bool = False

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        self.backend.inflight -= 1
        self.backend.reserved_mem_mb -= self.request.mem_mb

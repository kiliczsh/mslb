"""Clock and output collaborators for bounded real-time traffic."""

from typing import Protocol

from mslb_contracts.docker_runtime import HttpEvent

from mslb_controller.router import Router


class Clock(Protocol):
    """Separate live scheduling time from deterministic test clocks."""

    def now(self) -> float:
        """Return the current monotonic time in seconds."""
        ...

    async def pause(self, *, seconds: float) -> None:
        """Wait for a bounded scheduling interval measured in seconds."""
        ...


class EventSink(Protocol):
    """Append HTTP lifecycle events without coupling routing to artifact storage."""

    async def append(self, *, event: HttpEvent) -> None:
        """Append one HTTP lifecycle event to the attempt's bounded output stream."""
        ...


class Membership(Protocol):
    """Evaluate only the current trusted router and its bounded pending count."""

    async def __call__(self, *, router: Router, pending: int) -> None: ...


class RetireBackend(Protocol):
    """Resolve an exact owned backend identity before removing routing membership."""

    async def __call__(self, *, backend_id: str) -> None: ...

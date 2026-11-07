"""Clock and output collaborators for bounded real-time traffic."""

from typing import Protocol

from mslb_contracts.docker_runtime import HttpEvent

from mslb_controller.router import Router


class Clock(Protocol):
    def now(self) -> float: ...
    async def pause(self, *, seconds: float) -> None: ...


class EventSink(Protocol):
    async def append(self, *, event: HttpEvent) -> None: ...


class Membership(Protocol):
    """Evaluate only the current trusted router and its bounded pending count."""

    async def __call__(self, *, router: Router, pending: int) -> None: ...


class RetireBackend(Protocol):
    """Resolve an exact owned backend identity before removing routing membership."""

    async def __call__(self, *, backend_id: str) -> None: ...

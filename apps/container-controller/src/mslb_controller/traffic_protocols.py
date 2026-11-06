"""Clock and output collaborators for bounded real-time traffic."""

from typing import Protocol

from mslb_contracts.docker_runtime import HttpEvent


class Clock(Protocol):
    def now(self) -> float: ...
    async def pause(self, *, seconds: float) -> None: ...


class EventSink(Protocol):
    async def append(self, *, event: HttpEvent) -> None: ...

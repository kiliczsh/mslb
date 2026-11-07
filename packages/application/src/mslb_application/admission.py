"""Non-queuing admission for expensive application input materialization."""

import asyncio
from collections.abc import Awaitable, Callable

from mslb_application.errors import OperationError


async def settle[T](task: asyncio.Task[T]) -> T:
    """Await owned work without abandoning it during repeated cancellation."""
    try:
        return await asyncio.shield(arg=task)
    except asyncio.CancelledError:
        drained = asyncio.gather(task, return_exceptions=True)
        while not drained.done():
            try:
                await asyncio.shield(arg=drained)
            except asyncio.CancelledError:
                continue
        raise


class MaterializationAdmission:
    """One upload/snapshot operation per shared application instance, including cleanup."""

    def __init__(self) -> None:
        self.lock = asyncio.Lock()

    async def run[T](self, operation: Callable[[], Awaitable[T]]) -> T:
        """Materialize bounded work under the shared concurrency admission limit."""
        if self.lock.locked():
            raise OperationError(
                code="RATE_LIMITED", message="Input processing capacity is occupied"
            )
        async with self.lock:

            async def invoke() -> T:
                return await operation()

            task = asyncio.create_task(coro=invoke())
            # Threads cannot be cancelled safely; retain the slot through settlement.
            return await settle(task=task)

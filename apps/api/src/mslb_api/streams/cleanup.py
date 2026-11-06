"""Cancellation-safe completion of bounded stream filesystem work."""

import asyncio
from collections.abc import Callable


async def finish[T](*, operation: Callable[[], T]) -> T:
    """Do not abandon bounded filesystem work or cleanup on repeated cancellation."""
    task = asyncio.create_task(asyncio.to_thread(operation))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        drained = asyncio.gather(task, return_exceptions=True)
        while not drained.done():
            try:
                await asyncio.shield(drained)
            except asyncio.CancelledError:
                continue
        raise

"""Production monotonic time and cancellable traffic pacing."""

import asyncio
import time


class MonotonicClock:
    """Use the process monotonic clock for live traffic scheduling."""

    def now(self) -> float:
        """Return monotonic seconds unaffected by wall-clock adjustments."""
        return time.monotonic()

    async def pause(self, *, seconds: float) -> None:
        """Yield for the requested scheduling delay in seconds."""
        await asyncio.sleep(delay=seconds)

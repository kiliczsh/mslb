"""Production monotonic time and cancellable traffic pacing."""

import asyncio
import time


class MonotonicClock:
    def now(self) -> float:
        return time.monotonic()

    async def pause(self, *, seconds: float) -> None:
        await asyncio.sleep(delay=seconds)

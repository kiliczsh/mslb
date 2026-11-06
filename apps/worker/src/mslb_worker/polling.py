"""Durable polling with quiet retries and one safe diagnostic per continuous outage."""

import asyncio
from threading import Event

from mslb_application.diagnostics import report_failure
from mslb_application.worker import Worker
from mslb_persistence.errors import StoreError
from sqlalchemy.exc import DBAPIError

from mslb_worker.redis_notifications import RedisHints


async def poll(*, worker: Worker, hints: RedisHints, shutdown: Event, once: bool) -> None:
    """Retry known dependency failures; retain fail-fast one-shot and cancellation semantics."""
    unavailable = False
    while not shutdown.is_set():
        try:
            found = await worker.run_once(shutdown=shutdown)
        except (StoreError, DBAPIError, OSError, TimeoutError) as error:
            if once:
                raise
            if not unavailable:
                report_failure(error=error, code="RESOURCE_PRESSURE", source="worker_poll")
            unavailable = True
            await asyncio.sleep(delay=1)
            continue
        unavailable = False
        if once:
            break
        if not found:
            await hints.wait()

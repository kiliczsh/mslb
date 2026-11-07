"""Default durable controller service with bounded polling and independent liveness."""

import asyncio
import logging
import os
import signal
from pathlib import Path

from mslb_application.files import ManagedFiles, StorageLimits
from mslb_contracts.resources import parse_resource_guard
from mslb_persistence.database import Database

from mslb_controller.control_client import ControlClient
from mslb_controller.controller import FAILURES, Controller
from mslb_controller.controller_health import write_health

logger = logging.getLogger(name=__name__)


async def main() -> None:
    """Enroll trusted deployment, supervise durable work and leave expiry recovery authoritative."""
    database = Database(
        url=os.environ["DATABASE_URL"],
        schema=os.environ.get(key="CONTROL_DATABASE_SCHEMA", default="public"),
        enforce_resource_guard=parse_resource_guard(
            value=os.environ.get(key="RESOURCE_GUARD_ENABLED", default="true")
        ),
    )
    limits = StorageLimits()
    inputs = await asyncio.to_thread(
        ManagedFiles, root=Path("/data/inputs"), limits=limits, read_only=True
    )
    outputs = await asyncio.to_thread(ManagedFiles, root=Path("/data/outputs"), limits=limits)
    client = ControlClient()
    shutdown, done = asyncio.Event(), asyncio.Event()
    loop = asyncio.get_running_loop()
    for code in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig=code, callback=shutdown.set)
    controller = Controller(database=database, inputs=inputs, outputs=outputs, client=client)

    async def heartbeat() -> None:
        while not done.is_set():
            try:
                if await database.ready():
                    await asyncio.to_thread(write_health)
            except FAILURES:
                pass  # Old heartbeat expires; unavailable storage cannot fabricate readiness.
            try:
                await asyncio.wait_for(fut=done.wait(), timeout=1)
            except TimeoutError:
                continue

    previous: tuple[str | None, str | None] | None = None
    try:
        await client.enroll()
        health = asyncio.create_task(coro=heartbeat())
        try:
            while not shutdown.is_set():
                try:
                    claimed = await controller.run_once(shutdown=shutdown)
                    category, detail = controller.last_error_code, controller.last_failure
                except FAILURES as error:
                    claimed, category, detail = False, type(error).__name__, None
                if category is not None and (category, detail) != previous:
                    logger.warning(
                        "Controller ownership/cleanup unavailable (%s; %s)", category, detail
                    )
                previous = (category, detail)
                if not claimed:
                    try:
                        await asyncio.wait_for(fut=shutdown.wait(), timeout=0.5)
                    except TimeoutError:
                        continue
        finally:
            done.set()
            await health
    finally:
        await client.close()
        await database.close()


if __name__ == "__main__":
    asyncio.run(main=main())

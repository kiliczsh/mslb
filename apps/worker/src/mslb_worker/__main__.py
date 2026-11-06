"""Poll durable jobs without requiring Redis; handle process shutdown safely."""

import argparse
import asyncio
import os
import signal
import sys
from pathlib import Path
from threading import Event

from mslb_application.managed_files import ManagedFiles
from mslb_application.notifications import OutboxNotifications
from mslb_application.storage_values import StorageLimits
from mslb_application.worker import Worker
from mslb_persistence.database import Database
from mslb_persistence.jobs import Jobs

from mslb_worker.health import healthy, write_health
from mslb_worker.notifications import Notifications as Notifications
from mslb_worker.polling import poll
from mslb_worker.redis_notifications import RedisHints


async def main(*, once: bool) -> None:
    """Run supervised durable polling with bounded Redis hints and coordinated shutdown."""
    database = Database(
        url=os.environ["DATABASE_URL"], schema=os.environ.get("DATABASE_SCHEMA", "public")
    )
    limits = StorageLimits(
        min_free_bytes=int(os.environ.get("STORAGE_MIN_FREE_BYTES", str(2 * 1024**3)))
    )
    inputs = await asyncio.to_thread(
        ManagedFiles,
        root=Path(os.environ.get("INPUT_ROOT", "/data/inputs")),
        limits=limits,
        read_only=True,
    )
    outputs = await asyncio.to_thread(
        ManagedFiles, root=Path(os.environ.get("OUTPUT_ROOT", "/data/outputs")), limits=limits
    )
    notifications, shutdown, health_done = Notifications(), Event(), asyncio.Event()
    loop = asyncio.get_running_loop()
    for code in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(code, shutdown.set)
    worker = Worker(
        database=database,
        inputs=inputs,
        outputs=outputs,
        lease_seconds=int(os.environ.get("WORKER_LEASE_SECONDS", "30")),
        renewal_seconds=float(os.environ.get("WORKER_RENEW_SECONDS", "5")),
        observer=notifications.observe,
    )
    hints = RedisHints(
        url=os.environ.get("REDIS_URL", "redis://redis:6379/0"), namespace=database.schema
    )
    outbox = OutboxNotifications(jobs=Jobs(database=database), hints=hints)

    async def heartbeat() -> None:
        while not health_done.is_set():
            await asyncio.to_thread(write_health)
            try:
                await asyncio.wait_for(fut=health_done.wait(), timeout=1)
            except TimeoutError:
                continue

    health = asyncio.create_task(coro=heartbeat())
    delivery = asyncio.create_task(coro=outbox.run(done=health_done))
    try:
        await poll(worker=worker, hints=hints, shutdown=shutdown, once=once)
    finally:
        health_done.set()
        delivery.cancel()
        await asyncio.gather(delivery, return_exceptions=True)
        await health
        await hints.close()
        await database.close()
        await asyncio.to_thread(notifications.close)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", nargs="?", choices=("run", "health"), default="run")
    parser.add_argument("--once", action="store_true")
    arguments = parser.parse_args()
    if arguments.command == "health":
        sys.exit(0 if healthy() else 1)
    asyncio.run(main=main(once=bool(arguments.once)))

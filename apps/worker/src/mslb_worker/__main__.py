"""Poll durable jobs without requiring Redis; handle process shutdown safely."""

import argparse
import asyncio
import json
import os
import queue
import signal
import sys
from pathlib import Path
from threading import Event, Thread

from mslb_application.files import ManagedFiles, StorageLimits
from mslb_application.notifications import OutboxNotifications
from mslb_application.worker import Worker
from mslb_persistence.database import Database, StoreError
from mslb_persistence.jobs import Jobs
from mslb_persistence.records import Lease
from sqlalchemy.exc import DBAPIError

from mslb_worker.health import healthy, write_health
from mslb_worker.redis_notifications import RedisHints


class Notifications:
    def __init__(self) -> None:
        self.messages: queue.Queue[dict[str, str | int]] = queue.Queue(maxsize=64)
        self.done = Event()
        self.thread = Thread(target=self._write, daemon=True)
        self.thread.start()

    def observe(self, phase: str, lease: Lease, tick: int) -> None:
        try:
            self.messages.put_nowait(
                dict(
                    event=phase,
                    run_id=str(lease.run_id),
                    attempt=lease.attempt,
                    fence=lease.fence,
                    tick=tick,
                )
            )
        except queue.Full:
            return

    def _write(self) -> None:
        while not self.done.is_set() or not self.messages.empty():
            try:
                message = self.messages.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                print(json.dumps(obj=message, separators=(",", ":")), flush=True)
            except (OSError, ValueError):
                return

    def close(self) -> None:
        self.done.set()
        self.thread.join(timeout=2)


async def main(*, once: bool) -> None:
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
        while not shutdown.is_set():
            try:
                found = await worker.run_once(shutdown=shutdown)
            except (StoreError, DBAPIError, OSError, TimeoutError):
                if once:
                    raise
                await asyncio.sleep(delay=1)
                continue
            if once:
                break
            if not found:
                await hints.wait()
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

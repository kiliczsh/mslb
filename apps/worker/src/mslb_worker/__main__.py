"""Poll durable jobs without requiring Redis; handle process shutdown safely."""

import argparse
import asyncio
import json
import os
import queue
import signal
import sys
import tempfile
import time
from pathlib import Path
from threading import Event, Thread

from mslb_application.files import ManagedFiles, StorageLimits
from mslb_application.worker import Worker
from mslb_persistence.database import Database, StoreError
from mslb_persistence.records import Lease
from sqlalchemy.exc import DBAPIError

HEALTH = Path(tempfile.gettempdir()) / "mslb-worker" / "health.json"


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
                print(json.dumps(message, separators=(",", ":")), flush=True)
            except (OSError, ValueError):
                return

    def close(self) -> None:
        self.done.set()
        self.thread.join(timeout=2)


def healthy() -> bool:
    try:
        with HEALTH.open("rb") as stream:
            record = json.loads(stream.read(1024))
        value = record.get("updated")
        return isinstance(value, (float, int)) and 0 <= time.monotonic() - value < 10
    except (OSError, ValueError):
        return False


def write_health() -> None:
    if HEALTH.parent.is_symlink():
        raise ValueError("Unsafe worker health directory")
    HEALTH.parent.mkdir(mode=0o700, exist_ok=True)
    metadata = HEALTH.parent.stat()
    if metadata.st_uid != os.getuid() or metadata.st_mode & 0o077:
        raise ValueError("Worker health directory must be private")
    temporary = HEALTH.with_suffix(".tmp")
    temporary.write_text(json.dumps(dict(updated=time.monotonic())))
    temporary.replace(HEALTH)


async def main(once: bool) -> None:
    database = Database(os.environ["DATABASE_URL"], os.environ.get("DATABASE_SCHEMA", "public"))
    limits = StorageLimits(
        min_free_bytes=int(os.environ.get("STORAGE_MIN_FREE_BYTES", str(2 * 1024**3)))
    )
    inputs = await asyncio.to_thread(
        ManagedFiles, Path(os.environ.get("INPUT_ROOT", "/data/inputs")), limits, True
    )
    outputs = await asyncio.to_thread(
        ManagedFiles, Path(os.environ.get("OUTPUT_ROOT", "/data/outputs")), limits
    )
    notifications, shutdown, health_done = Notifications(), Event(), asyncio.Event()
    loop = asyncio.get_running_loop()
    for code in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(code, shutdown.set)
    worker = Worker(
        database,
        inputs,
        outputs,
        lease_seconds=int(os.environ.get("WORKER_LEASE_SECONDS", "30")),
        renewal_seconds=float(os.environ.get("WORKER_RENEW_SECONDS", "5")),
        observer=notifications.observe,
    )

    async def heartbeat() -> None:
        while not health_done.is_set():
            await asyncio.to_thread(write_health)
            try:
                await asyncio.wait_for(health_done.wait(), timeout=1)
            except TimeoutError:
                continue

    health = asyncio.create_task(heartbeat())
    try:
        while not shutdown.is_set():
            try:
                found = await worker.run_once(shutdown)
            except (StoreError, DBAPIError, OSError, TimeoutError):
                if once:
                    raise
                await asyncio.sleep(1)
                continue
            if once:
                break
            if not found:
                await asyncio.sleep(0.25)
    finally:
        health_done.set()
        await health
        await database.close()
        await asyncio.to_thread(notifications.close)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", nargs="?", choices=("run", "health"), default="run")
    parser.add_argument("--once", action="store_true")
    arguments = parser.parse_args()
    if arguments.command == "health":
        sys.exit(0 if healthy() else 1)
    asyncio.run(main(bool(arguments.once)))

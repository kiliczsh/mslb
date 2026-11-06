"""Bounded operational notifications independent of durable worker execution."""

import json
import queue
from threading import Event, Thread

from mslb_persistence.records import Lease


class Notifications:
    def __init__(self) -> None:
        self.messages: queue.Queue[dict[str, str | int]] = queue.Queue(maxsize=64)
        self.done = Event()
        self.thread = Thread(target=self._write, daemon=True)
        self.thread.start()

    def observe(self, phase: str, lease: Lease, tick: int) -> None:
        try:
            self.messages.put_nowait(
                item={
                    "event": phase,
                    "run_id": str(lease.run_id),
                    "attempt": lease.attempt,
                    "fence": lease.fence,
                    "tick": tick,
                }
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

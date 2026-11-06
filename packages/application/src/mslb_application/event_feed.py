"""Bounded committed-event replay with a separate operational progress channel."""

import asyncio
from collections.abc import AsyncGenerator
from uuid import UUID

from mslb_contracts.queries import RunView
from mslb_persistence.database import Database, StoreError
from mslb_persistence.durability import Durability
from mslb_persistence.reads import Reads
from sqlalchemy.exc import DBAPIError

from mslb_application.errors import OperationError


class EventFeed:
    def __init__(self, database: Database, run_id: UUID, after_sequence: int = 0) -> None:
        if type(after_sequence) is not int or not 0 <= after_sequence <= 30_000:
            raise OperationError("VALIDATION_ERROR", "Invalid Last-Event-ID")
        self.reads, self.events = Reads(database), Durability(database)
        self.run_id, self.sequence = run_id, after_sequence
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=500)
        self.queued_bytes = 0
        self.closed = asyncio.Event()
        self.overflow = False

    def offer(self, frame: bytes) -> bool:
        if self.queued_bytes + len(frame) > 64 * 1024 or self.queue.full():
            self.overflow = True
            self.closed.set()
            return False
        self.queue.put_nowait(frame)
        self.queued_bytes += len(frame)
        return True

    async def produce(self, initial: RunView) -> None:
        previous = ""
        run = initial
        try:
            while True:
                page = await self.events.events(self.run_id, self.sequence, limit=100)
                for event in page:
                    frame = (
                        f"id: {event.sequence}\nevent: trace\ndata: {event.model_dump_json()}\n\n"
                    ).encode()
                    if not self.offer(frame):
                        return
                    self.sequence = event.sequence
                progress = run.model_dump_json(
                    exclude={
                        "configuration",
                        "dataset_id",
                        "dataset_digest",
                        "server_snapshot_digest",
                        "engine_version",
                        "snapshot_digest",
                        "created_at",
                    }
                )
                if progress != previous:
                    if not self.offer(f"event: progress\ndata: {progress}\n\n".encode()):
                        return
                    previous = progress
                if len(page) == 100:
                    run = await self.reads.run(self.run_id)
                    continue
                run = await self.reads.run(self.run_id)
                # Re-read after observing terminal status to cover a completion
                # transaction racing the preceding event page.
                if run.status in {"completed", "failed", "cancelled"}:
                    tail = await self.events.events(self.run_id, self.sequence, limit=1)
                    if tail:
                        continue
                    final = run.model_dump_json(exclude={"configuration"})
                    self.offer(f"event: status\ndata: {final}\n\n".encode())
                    return
                await asyncio.sleep(0.25)
        except (StoreError, DBAPIError, OSError, TimeoutError):
            self.offer(b'event: error\ndata: {"code":"RESOURCE_PRESSURE"}\n\n')
        finally:
            self.closed.set()

    async def stream(self, initial: RunView) -> AsyncGenerator[bytes]:
        producer = asyncio.create_task(self.produce(initial))
        try:
            while not self.closed.is_set() or not self.queue.empty():
                try:
                    frame = await asyncio.wait_for(self.queue.get(), timeout=1)
                except TimeoutError:
                    if producer.done():
                        await producer
                    yield b": heartbeat\n\n"
                    continue
                self.queued_bytes -= len(frame)
                yield frame
            await producer
            if self.overflow:
                yield b'event: replay_required\ndata: {"reason":"slow_consumer"}\n\n'
        finally:
            producer.cancel()
            await asyncio.gather(producer, return_exceptions=True)

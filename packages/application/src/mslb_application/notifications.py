"""Best-effort wakeups; neither delivery nor consumption transfers job ownership."""

import asyncio
from typing import Protocol
from uuid import UUID

from mslb_persistence.database import StoreError
from sqlalchemy.exc import DBAPIError


class WakeupHints(Protocol):
    async def publish(self, *, run_id: UUID) -> bool:
        """Return true only when the ephemeral hint was accepted."""
        ...

    async def wait(self) -> None:
        """Wait at most 250ms; callers must always claim from PostgreSQL next."""
        ...


class NotificationOutbox(Protocol):
    async def pending_outbox(self, *, limit: int) -> tuple[tuple[UUID, UUID], ...]: ...

    async def mark_outbox_delivered(self, *, message_id: UUID) -> None: ...

    async def prune_outbox(self) -> None: ...


class OutboxNotifications:
    def __init__(self, *, jobs: NotificationOutbox, hints: WakeupHints) -> None:
        self.jobs, self.hints = jobs, hints

    async def dispatch_once(self) -> int:
        """Publish one bounded page; acknowledge only successful deliveries."""
        delivered = 0
        for message_id, run_id in await self.jobs.pending_outbox(limit=20):
            if not await self.hints.publish(run_id=run_id):
                break
            await self.jobs.mark_outbox_delivered(message_id=message_id)
            delivered += 1
        return delivered

    async def run(self, *, done: asyncio.Event) -> None:
        """Retry independently of execution; stale hints expire even during an outage."""
        next_prune = 0.0
        loop = asyncio.get_running_loop()
        while not done.is_set():
            try:
                if loop.time() >= next_prune:
                    await self.jobs.prune_outbox()
                    next_prune = loop.time() + 60
                await self.dispatch_once()
            except (StoreError, DBAPIError, OSError, TimeoutError):
                # A failed DB acknowledgment permits duplicate hints. Claims still
                # use the durable execution slot and fencing predicates.
                pass
            try:
                await asyncio.wait_for(fut=done.wait(), timeout=1)
            except TimeoutError:
                continue

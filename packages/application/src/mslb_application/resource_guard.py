"""Sequential background evidence publication with fenced ownership and bounded recovery."""

import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from mslb_contracts.resources import ResourceProbe
from mslb_persistence.database import StoreError
from mslb_persistence.resource_evidence import (
    EvidenceLease,
    EvidenceRecord,
    EvidenceRejected,
    ResourceEvidenceStore,
)
from sqlalchemy.exc import DBAPIError

from mslb_application.resource_policy import ResourcePolicy


class ResourcePublisher:
    """Publish bounded measured admission decisions without owning workload execution."""

    def __init__(
        self,
        *,
        store: ResourceEvidenceStore,
        owner: str,
        measure: Callable[[], Awaitable[ResourceProbe | None]],
    ) -> None:
        self.store, self.owner, self.measure = store, owner, measure
        self.lease: EvidenceLease | None = None
        self.revision = 0
        self.policy = ResourcePolicy()
        self.busy = False

    def reset(self) -> None:
        """Reset publisher ownership/revision and fail-closed admission warmup."""
        self.lease = None
        self.revision = 0
        self.policy = ResourcePolicy()

    async def publish_once(self) -> EvidenceRecord | None:
        """Sample only after winning ownership; never queue overlapping sample requests."""
        if self.busy:
            raise StoreError(code="RATE_LIMITED", message="Resource publisher is occupied")
        self.busy = True
        try:
            if self.lease is None:
                self.lease = await self.store.claim(owner=self.owner)
                if self.lease is None:
                    return None
                self.revision = self.lease.revision
                self.policy = ResourcePolicy()
            try:
                async with asyncio.timeout(delay=5):
                    probe = await self.measure()
            except (OSError, TimeoutError):
                probe = None
            decision = self.policy.evaluate(probe=probe, now=datetime.now(tz=UTC))
            result = await self.store.publish(
                lease=self.lease,
                expected_revision=self.revision,
                probe=probe,
                decision=decision,
            )
            self.revision = result.revision
            return result
        except EvidenceRejected:
            # The input was rejected before UPDATE and the transaction rolled back.
            # Keep acknowledged ownership, but rebuild warmup from fresh measurements.
            self.policy = ResourcePolicy()
            raise
        except BaseException:
            # A commit receipt may have been lost. Reclaim with a new fence after expiry,
            # rather than guessing a revision or retaining pre-failure recovery samples.
            self.reset()
            raise
        finally:
            self.busy = False

    async def invalidate(self) -> None:
        """Best-effort fail-closed publication on orderly shutdown; expiry remains the fallback."""
        if self.lease is not None:
            try:
                await self.store.publish(
                    lease=self.lease,
                    expected_revision=self.revision,
                    probe=None,
                    decision=ResourcePolicy().evaluate(probe=None, now=datetime.now(tz=UTC)),
                )
            except (StoreError, DBAPIError, OSError, TimeoutError):
                pass
            finally:
                self.reset()

    async def run(
        self,
        *,
        stopped: asyncio.Event,
        published: Callable[[EvidenceRecord], Awaitable[None]] | None = None,
    ) -> None:
        """Run at a one-second cadence, without overlapping work or accumulating a backlog."""
        try:
            while not stopped.is_set():
                started = time.monotonic()
                try:
                    report = await self.publish_once()
                    if report is not None and published is not None:
                        await published(report)
                except (StoreError, DBAPIError, OSError, TimeoutError):
                    # publish_once owns lease recovery. A heartbeat error after an
                    # acknowledged publication must not discard that ownership.
                    pass
                try:
                    await asyncio.wait_for(
                        fut=stopped.wait(),
                        timeout=max(0.1, 1 - (time.monotonic() - started)),
                    )
                except TimeoutError:
                    continue
        finally:
            await self.invalidate()

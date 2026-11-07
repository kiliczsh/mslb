"""Connection-independent run queries with bounded, read-only batch waiting."""

import asyncio
from typing import Protocol
from uuid import UUID

from mslb_contracts.mcp import WaitArguments
from mslb_contracts.queries import PublishedValidation, RunStatus, WaitResult

from mslb_application.queries import Queries


class ProgressReporter(Protocol):
    """Report bounded batch-wait progress without owning or scheduling simulations."""

    async def __call__(self, *, progress: int, message: str) -> None: ...


TERMINAL = frozenset({"completed", "failed", "cancelled"})
POLL_SECONDS = 0.5
PROGRESS_SECONDS = 1.0
READ_SECONDS = 2.0


class RunWaiter:
    """Keep at most eight snapshots/receipts; never acquire worker ownership."""

    def __init__(self, *, queries: Queries) -> None:
        self.queries = queries

    async def wait(
        self,
        *,
        run_ids: tuple[UUID, ...],
        wait_seconds: int,
        report: ProgressReporter | None = None,
    ) -> WaitResult:
        # Validate here too: internal callers cannot accidentally create unbounded waits.
        """Wait within a bounded batch/time budget and preserve cancellation semantics."""
        arguments = WaitArguments(run_ids=run_ids, wait_seconds=wait_seconds)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + arguments.wait_seconds
        verified: dict[UUID, PublishedValidation] = {}
        reported: tuple[RunStatus, ...] | None = None
        last_report, ordinal = float("-inf"), 0
        while True:
            async with asyncio.timeout(delay=READ_SECONDS):
                statuses = await self.snapshot(run_ids=arguments.run_ids, verified=verified)
            now = loop.time()
            terminal = all(row.status in TERMINAL for row in statuses)
            if (
                report is not None
                and statuses != reported
                and now - last_report >= PROGRESS_SECONDS
            ):
                ordinal += 1
                async with asyncio.timeout(delay=1.0):
                    await report(progress=ordinal, message=self.message(statuses=statuses))
                reported, last_report = statuses, loop.time()
            if terminal or now >= deadline:
                return WaitResult(statuses=statuses, all_terminal=terminal, timed_out=not terminal)
            await asyncio.sleep(delay=min(POLL_SECONDS, max(0.0, deadline - loop.time())))

    async def snapshot(
        self,
        *,
        run_ids: tuple[UUID, ...],
        verified: dict[UUID, PublishedValidation],
    ) -> tuple[RunStatus, ...]:
        """Read compact durable statuses and published receipts for one bounded batch."""
        rows = await self.queries.reads.runs_by_ids(run_ids=run_ids)
        result = []
        for row in rows:
            if row.status == "completed" and row.id not in verified:
                # The worker publishes only after independent validation. Reuse the
                # normal query's manifest/hash/identity checks before claiming success.
                _, manifest, _ = await self.queries.completed(run_id=row.id)
                verified[row.id] = PublishedValidation(
                    bundle_digest=manifest.bundle_digest,
                    independent_replay=manifest.independent_replay,
                    original_validator=manifest.original_validator,
                )
            result.append(
                RunStatus(
                    run_id=row.id,
                    status=row.status,
                    attempts=row.attempts,
                    error_code=row.error_code,
                    progress=row.progress,
                    validation=verified.get(row.id) if row.status == "completed" else None,
                )
            )
        return tuple(result)

    @staticmethod
    def message(*, statuses: tuple[RunStatus, ...]) -> str:
        """Format a bounded progress update for the current batch wait."""
        return "Persisted runs: " + "; ".join(
            f"{str(object=row.run_id)[:8]} {row.status}, attempt {row.attempts}"
            + (
                f", tick {row.progress.next_tick}, {row.progress.committed_events} committed events"
                if row.progress is not None
                else ""
            )
            + (", validation and publication passed" if row.validation is not None else "")
            for row in statuses
        )

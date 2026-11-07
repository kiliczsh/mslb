"""Supervised logical execution; PostgreSQL remains the sole job authority."""

import asyncio
import errno
import os
import re
import time
from collections.abc import Callable
from contextlib import ExitStack
from threading import Event
from uuid import uuid4

from mslb_contracts.capacity_profiles import request_limit
from mslb_contracts.inputs import InputError
from mslb_contracts.runs import stored_request_limit
from mslb_engine.models import SimulationError
from mslb_engine.replay import ReplayError
from mslb_persistence.catalog import Catalog
from mslb_persistence.database import Database, StoreError
from mslb_persistence.durability import Durability
from mslb_persistence.jobs import Jobs
from mslb_persistence.records import Lease
from mslb_persistence.resource_evidence import ResourceEvidenceStore
from pydantic import ValidationError
from sqlalchemy.exc import DBAPIError

from mslb_application.checkpoint_values import PREFIX_NAMES
from mslb_application.errors import OperationError
from mslb_application.journal import Journal, on_loop
from mslb_application.managed_files import ManagedFiles
from mslb_application.worker_control import ExecutionStopped as ExecutionStopped
from mslb_application.worker_control import _Control as _Control


class Worker:
    """Supervise one logical attempt with independent lease, cancellation and deadline checks."""

    def __init__(
        self,
        database: Database,
        inputs: ManagedFiles,
        outputs: ManagedFiles,
        owner: str | None = None,
        lease_seconds: int = 30,
        renewal_seconds: float = 5,
        observer: Callable[[str, Lease, int], None] | None = None,
    ) -> None:
        if (
            type(lease_seconds) is not int
            or not 1 <= lease_seconds <= 300
            or not 0 < renewal_seconds < lease_seconds / 2
        ):
            raise OperationError(
                code="VALIDATION_ERROR", message="Invalid worker lease/renewal policy"
            )
        self.database, self.inputs, self.outputs = database, inputs, outputs
        self.resource_evidence = ResourceEvidenceStore(database=database)
        self.jobs, self.catalog, self.durable = (
            Jobs(database=database),
            Catalog(database=database),
            Durability(database=database),
        )
        self.owner, self.lease_seconds, self.renewal_seconds = (
            owner or str(object=uuid4()),
            lease_seconds,
            renewal_seconds,
        )
        self.observer = observer
        self.dropped_notifications = 0

    def notify(self, phase: str, lease: Lease, tick: int = 0) -> None:
        """Notify the optional phase observer; count and isolate callback failures."""
        if self.observer is not None:
            try:
                self.observer(phase, lease, tick)
            except Exception:
                # The optional operational notification boundary cannot change
                # canonical execution; keep a bounded failure count for diagnostics.
                self.dropped_notifications = min(self.dropped_notifications + 1, 1_000_000)

    def _execute(self, lease: Lease, loop: asyncio.AbstractEventLoop, control: _Control) -> None:
        """Run or restore one immutable attempt outside the async supervision loop."""
        run = on_loop(loop=loop, operation=self.catalog.get_run(run_id=lease.run_id))
        snapshot = self.inputs.load_snapshot(
            key=run.snapshot_key, expected_digest=run.snapshot_digest
        )
        if stored_request_limit(settings=snapshot.configuration) > request_limit(
            profile=self.database.capacity_profile
        ):
            raise OperationError(
                code="RESOURCE_LIMIT_EXCEEDED", message="Run exceeds worker installation capacity"
            )
        recovery = on_loop(loop=loop, operation=self.durable.recovery(lease=lease))
        self._retired_spools(lease=lease, loop=loop, control=control)
        with ExitStack() as stack:
            journal = Journal(
                files=self.outputs,
                snapshot=snapshot,
                lease=lease,
                repository=self.durable,
                loop=loop,
                check=control.check,
                stack=stack,
                recovery=recovery,
            )
            if recovery is not None:
                self.notify(
                    phase="recovered_" + recovery.kind, lease=lease, tick=recovery.next_tick
                )
            if journal.seal is None:
                simulation = journal.simulation
                if simulation is None:
                    raise OperationError(
                        code="INVALID_STATE", message="Recovered execution state is missing"
                    )
                iterator = simulation.iter_ticks()
                while not simulation.terminal:
                    control.check()
                    if control.reason == "shutdown":
                        previous = (
                            journal.last_checkpoint.executed_intervals
                            if journal.last_checkpoint is not None
                            else 0
                        )
                        if simulation.checkpoint_ready and simulation.executed_intervals > previous:
                            journal.commit()
                            self.notify(
                                phase="checkpoint_committed", lease=lease, tick=simulation.tick
                            )
                        raise ExecutionStopped("shutdown")
                    frame = next(iterator)
                    journal.append(frame=frame)
                    if frame.terminal or journal.checkpoint_needed():
                        journal.commit()
                        self.notify(
                            phase="terminal_committed"
                            if frame.terminal
                            else "checkpoint_committed",
                            lease=lease,
                            tick=simulation.tick,
                        )
            if control.reason == "shutdown":
                raise ExecutionStopped("shutdown")
            reference, manifest = journal.finalize()
            self.notify(phase="candidate_published", lease=lease, tick=manifest.terminal_tick)
            control.check()
            if control.reason == "shutdown":
                raise ExecutionStopped("shutdown")
            on_loop(
                loop=loop,
                operation=self.durable.complete(
                    lease=lease, reference=reference, manifest=manifest
                ),
            )
            self.notify(phase="completed", lease=lease, tick=manifest.terminal_tick)

    def _retired_spools(
        self, lease: Lease, loop: asyncio.AbstractEventLoop, control: _Control
    ) -> None:
        """Remove only unreferenced exact spools from retired attempts."""
        directory = self.outputs.path(key=f"runs/{lease.run_id}")
        if not directory.is_dir():
            return
        with os.scandir(path=directory) as entries:
            for number, entry in enumerate(iterable=entries):
                if number >= 16:
                    raise OperationError(
                        code="VALIDATION_FAILED",
                        message="Attempt namespace contains unexpected entries",
                    )
                match = re.fullmatch(pattern=r"attempt-([0-9]{1,19})", string=entry.name)
                if match is None or int(match[1]) >= lease.fence:
                    continue
                for name in PREFIX_NAMES:
                    control.check()
                    key = f"runs/{lease.run_id}/{entry.name}/work/{name}.part"
                    path = self.outputs.path(key=key)
                    if path.exists() and on_loop(
                        loop=loop, operation=self.durable.removable(lease=lease, key=key)
                    ):
                        self.outputs.remove(key=key)

    async def _watch(
        self,
        lease: Lease,
        control: _Control,
        done: asyncio.Event,
        shutdown: Event,
        claim_started: float,
    ) -> None:
        """Renew ownership and observe cancellation independently of CPU execution."""
        next_renewal = claim_started + self.renewal_seconds
        next_resource_check = claim_started
        deadline = claim_started + self.lease_seconds - min(1, self.lease_seconds / 4)
        control.deadline = deadline
        while not done.is_set():
            if shutdown.is_set():
                control.stop(reason="shutdown")
            now = time.monotonic()
            if now >= deadline:
                control.stop(reason="lease_lost")
                return
            try:
                started = time.monotonic()
                timeout = min(2, max(0.01, deadline - started))
                if started >= next_renewal:
                    state = await asyncio.wait_for(
                        fut=self.jobs.renew(lease=lease, lease_seconds=self.lease_seconds),
                        timeout=timeout,
                    )
                    deadline = started + self.lease_seconds - min(1, self.lease_seconds / 4)
                    control.deadline = deadline
                    next_renewal = started + self.renewal_seconds
                else:
                    state = await asyncio.wait_for(
                        fut=self.jobs.get(run_id=lease.run_id), timeout=timeout
                    )
                if state.fence != lease.fence or state.attempts != lease.attempt:
                    control.stop(reason="lease_lost")
                    return
                if state.status == "cancel_requested":
                    control.stop(reason="cancel")
                elif state.status not in {"running", "completed"}:
                    control.stop(reason="lease_lost")
                    return
                if self.database.enforce_resource_guard and started >= next_resource_check:
                    evidence = await asyncio.wait_for(
                        fut=self.resource_evidence.read(), timeout=timeout
                    )
                    next_resource_check = started + 1
                    if evidence.decision.stop_running:
                        control.stop(reason="resource_pressure")
            except (StoreError, DBAPIError, TimeoutError, OSError):
                control.stop(reason="lease_lost")
                return
            try:
                await asyncio.wait_for(fut=done.wait(), timeout=0.25)
            except TimeoutError:
                continue

    async def _deadline(self, control: _Control, done: asyncio.Event, shutdown: Event) -> None:
        """Remain independent of a query waiting for network cancellation."""
        while not done.is_set():
            if shutdown.is_set():
                control.stop(reason="shutdown")
            if time.monotonic() >= control.deadline:
                control.stop(reason="lease_lost")
                return
            try:
                await asyncio.wait_for(fut=done.wait(), timeout=0.1)
            except TimeoutError:
                continue

    async def _failure(self, lease: Lease, code: str, retryable: bool = False) -> None:
        try:
            await self.jobs.fail(lease=lease, error_code=code, retryable=retryable)
        except (StoreError, DBAPIError, OSError):
            # Unknown/lost ownership cannot authorize a substitute write. The
            # persisted lease and next claimant reconcile an unavailable DB.
            self.notify(phase="ownership_unavailable", lease=lease)

    async def run_once(self, shutdown: Event | None = None) -> bool:
        """Claim one job, supervise execution and persist an honest terminal or retry outcome."""
        shutdown = shutdown or Event()
        if shutdown.is_set():
            return False
        claim_started = time.monotonic()
        lease = await self.jobs.claim(owner=self.owner, lease_seconds=self.lease_seconds)
        if lease is None:
            return False
        self.notify(phase="claimed", lease=lease)
        control, done = _Control(), asyncio.Event()
        watcher = asyncio.create_task(
            coro=self._watch(
                lease=lease,
                control=control,
                done=done,
                shutdown=shutdown,
                claim_started=claim_started,
            )
        )
        deadline = asyncio.create_task(
            coro=self._deadline(control=control, done=done, shutdown=shutdown)
        )
        try:
            await asyncio.to_thread(
                self._execute, lease=lease, loop=asyncio.get_running_loop(), control=control
            )
        except ExecutionStopped as stopped:
            reason = str(object=stopped)
            if reason == "cancel":
                try:
                    await self.jobs.acknowledge_cancel(lease=lease)
                except (StoreError, DBAPIError, OSError):
                    self.notify(phase="ownership_unavailable", lease=lease)
            elif reason == "shutdown":
                await self._failure(lease=lease, code="INFRASTRUCTURE_FAILURE", retryable=True)
            elif reason == "resource_pressure":
                await self._failure(lease=lease, code="RESOURCE_PRESSURE")
            self.notify(phase="stopped_" + reason, lease=lease)
        except StoreError as error:
            if error.code == "INTERRUPTED":
                await self._failure(lease=lease, code="INTERNAL_ERROR")
            elif error.code == "RESOURCE_PRESSURE":
                await self._failure(lease=lease, code="RESOURCE_PRESSURE")
            elif error.code != "FENCE_LOST":
                await self._failure(lease=lease, code="VALIDATION_FAILED")
            self.notify(phase="stopped_" + error.code.lower(), lease=lease)
        except (
            SimulationError,
            OperationError,
            InputError,
            ReplayError,
            ValidationError,
            OSError,
            DBAPIError,
            TimeoutError,
        ) as error:
            code, retryable = "VALIDATION_FAILED", False
            if isinstance(error, (DBAPIError, TimeoutError)):
                code, retryable = "INFRASTRUCTURE_FAILURE", True
            elif isinstance(error, OSError):
                if error.errno in {errno.ENOSPC, errno.EDQUOT, errno.EACCES, errno.EROFS}:
                    code = "STORAGE_LIMIT_EXCEEDED"
                elif error.errno != errno.ENOENT:
                    code, retryable = "INFRASTRUCTURE_FAILURE", True
            elif isinstance(error, (SimulationError, OperationError)):
                code = error.code
                if code == "PAYLOAD_TOO_LARGE":
                    code = "STORAGE_LIMIT_EXCEEDED"
                if code not in {
                    "RESOURCE_PRESSURE",
                    "SIMULATION_LIMIT_EXCEEDED",
                    "RESOURCE_LIMIT_EXCEEDED",
                    "STORAGE_LIMIT_EXCEEDED",
                    "VALIDATION_FAILED",
                }:
                    code = "VALIDATION_FAILED"
            await self._failure(lease=lease, code=code, retryable=retryable)
            self.notify(phase="failed_" + code.lower(), lease=lease)
        finally:
            done.set()
            await watcher
            await deadline
        return True

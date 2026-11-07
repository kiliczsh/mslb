"""Durable single-run supervision over restricted lifecycle RPC and actual HTTP traffic."""

import asyncio
import time
from dataclasses import dataclass, field
from uuid import uuid4

from mslb_application.docker_inputs import load_docker_snapshot
from mslb_application.docker_outputs import DockerOutputs
from mslb_application.errors import OperationError
from mslb_application.managed_files import ManagedFiles
from mslb_contracts.docker_runtime import StopReason
from mslb_persistence.catalog import Catalog
from mslb_persistence.database import Database, StoreError
from mslb_persistence.docker_catalog import DockerCatalog
from mslb_persistence.docker_control import RESOLVED, DockerControl
from mslb_persistence.docker_progress import DockerProgressStore
from mslb_persistence.docker_records import ControlLease
from mslb_persistence.docker_resources import DockerResources
from mslb_persistence.jobs import Jobs
from mslb_persistence.resource_evidence import ResourceEvidenceStore
from sqlalchemy.exc import DBAPIError

from mslb_controller.backend_http import BackendHttp
from mslb_controller.control_client import ControlClient
from mslb_controller.errors import EngineError, TrafficError
from mslb_controller.physical_resources import PhysicalResources, ResourceObserver, initial_backends
from mslb_controller.physical_scaling import PhysicalScaling
from mslb_controller.router import Router
from mslb_controller.runtime_observations import RuntimeObservations
from mslb_controller.traffic import TrafficDriver

FAILURES = (
    StoreError,
    OperationError,
    EngineError,
    TrafficError,
    DBAPIError,
    OSError,
    TimeoutError,
)


@dataclass(slots=True, kw_only=True)
class Attempt:
    """Share current phase, stop priority and an independent conservative renewal deadline."""

    lease: ControlLease
    deadline: float
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    reason: StopReason | None = None
    failure: str = "CONTROLLER_FAILURE"
    owns: bool = True

    def current(self) -> ControlLease:
        """Return the latest monitored lease reference for fenced control requests."""
        return self.lease

    def stopped(self) -> StopReason | None:
        """Return the recorded stop reason, or None while the attempt may continue."""
        return self.reason

    def stop(self, *, reason: StopReason, failure: str, owns: bool = True) -> None:
        """Signal that this attempt must stop admitting and sending traffic."""
        if not owns or reason == "CANCELLED" or self.reason is None:
            self.reason, self.failure = reason, failure
        self.owns = self.owns and owns


class Controller:
    """Claim once, never replay real calls, and retain uncertain resources for fenced cleanup."""

    def __init__(
        self,
        *,
        database: Database,
        inputs: ManagedFiles,
        outputs: ManagedFiles,
        client: ControlClient,
        owner: str | None = None,
    ) -> None:
        self.database, self.inputs, self.outputs, self.client = database, inputs, outputs, client
        self.owner = owner or uuid4().hex
        self.control = DockerControl(database=database)
        self.resources = DockerResources(database=database)
        self.catalog = DockerCatalog(database=database)
        self.jobs = Jobs(database=database)
        self.evidence = ResourceEvidenceStore(database=database)
        self.last_error_code: str | None = None
        self.last_failure: str | None = None

    async def _watch(self, *, state: Attempt, done: asyncio.Event, shutdown: asyncio.Event) -> None:
        next_renewal = time.monotonic() + 5
        while not done.is_set():
            now = time.monotonic()
            if now >= state.deadline:
                state.stop(reason="FENCE_LOST", failure="INTERRUPTED", owns=False)
                return
            if shutdown.is_set():
                state.stop(reason="FENCE_LOST", failure="INFRASTRUCTURE_FAILURE")
            try:
                if now >= next_renewal:
                    async with state.lock:
                        state.lease = await asyncio.wait_for(
                            fut=self.control.renew(lease=state.lease), timeout=2
                        )
                    state.deadline = now + 25
                    next_renewal = now + 5
                job = await asyncio.wait_for(
                    fut=self.jobs.get(run_id=state.lease.run_id), timeout=2
                )
                if job.status == "cancel_requested":
                    state.stop(reason="CANCELLED", failure="INTERRUPTED")
                if self.database.enforce_resource_guard:
                    report = await asyncio.wait_for(fut=self.evidence.read(), timeout=2)
                    if report.decision.stop_running:
                        state.stop(reason="RESOURCE_PRESSURE", failure="RESOURCE_PRESSURE")
            except FAILURES:
                state.stop(reason="FENCE_LOST", failure="INTERRUPTED", owns=False)
                return
            try:
                await asyncio.wait_for(fut=done.wait(), timeout=0.5)
            except TimeoutError:
                continue

    async def _cleanup(self, *, state: Attempt, observe: ResourceObserver | None = None) -> None:
        """Use exact persisted IDs; resolved resources never authorize another daemon mutation."""
        for resource in await self.resources.list(lease=state.lease):
            if resource.state in RESOLVED:
                continue
            await self.client.resource(lease=state.lease, identifier=resource.id, action="cleanup")
            if observe is not None:
                await observe(
                    resource=await self.resources.get(lease=state.lease, resource_id=resource.id)
                )

    async def _begin(self, *, state: Attempt, outcome: str, error_code: str | None = None) -> None:
        async with state.lock:
            if outcome == "completed":
                state.lease = await self.control.begin_cleanup(
                    lease=state.lease, outcome="completed"
                )
            elif outcome == "cancelled":
                state.lease = await self.control.begin_cleanup(
                    lease=state.lease, outcome="cancelled"
                )
            else:
                state.lease = await self.control.begin_cleanup(
                    lease=state.lease, outcome="failed", error_code=error_code
                )

    async def _execute(self, *, state: Attempt) -> None:
        run = await Catalog(database=self.database).get_run(run_id=state.lease.run_id)
        snapshot = await asyncio.to_thread(
            load_docker_snapshot,
            files=self.inputs,
            key=run.snapshot_key,
            expected_digest=run.snapshot_digest,
        )
        if snapshot != await self.catalog.snapshot(run_id=run.id):
            raise OperationError(code="VALIDATION_FAILED", message="Operational snapshots differ")
        dataset = await Catalog(database=self.database).get_dataset(dataset_id=snapshot.dataset_id)
        requests = await asyncio.to_thread(
            self.inputs.load_requests, digest=dataset.digest, size_bytes=dataset.size_bytes
        )
        settings = snapshot.configuration
        http = BackendHttp(
            request_timeout_ms=settings.request_timeout_ms,
            health_timeout_ms=settings.health_timeout_ms,
        )
        try:
            async with DockerOutputs(
                files=self.outputs,
                run_id=run.id,
                fence=state.lease.fence,
                snapshot=snapshot,
                requests=requests,
            ) as outputs:
                physical = PhysicalResources(
                    resources=self.resources,
                    client=self.client,
                    http=http,
                    settings=settings,
                    lease=state.current,
                    stop=state.stopped,
                    observe=outputs.observe,
                )
                initial = await initial_backends(
                    count=settings.initial_replicas,
                    provision=physical.provision,
                    check=physical.check,
                )
                scaling = PhysicalScaling(
                    settings=settings,
                    provision=physical.provision,
                    retire=physical.retire,
                    clock=time.monotonic,
                )
                observations = RuntimeObservations()
                progress = DockerProgressStore(database=self.database)

                async def update(*, router: Router, pending: int) -> None:
                    await scaling.evaluate(router=router, pending=pending)
                    if state.owns and state.stopped() is None:
                        written = await progress.write(
                            lease=state.lease,
                            frame=observations.progress(
                                router=router,
                                pending=pending,
                                elapsed=driver.elapsed(),
                                run_id=run.id,
                                fence=state.lease.fence,
                                snapshot_digest=snapshot.digest(),
                            ),
                        )
                        if not written:
                            state.stop(reason="CANCELLED", failure="INTERRUPTED")

                driver = TrafficDriver(
                    configuration=settings,
                    specs=tuple(ready.spec for ready in initial),
                    http=http,
                    sink=outputs,
                    stop=state.stopped,
                    membership=update,
                    health_observer=observations.sample,
                )
                summary = await driver.execute(requests=requests)
                if summary.status == "completed" and state.owns:
                    await driver.router.refresh()
                    await update(router=driver.router, pending=0)
                if not state.owns:
                    return
                await self._begin(
                    state=state,
                    outcome=summary.status,
                    error_code=state.failure if summary.status == "failed" else None,
                )
                await self._cleanup(state=state, observe=outputs.observe)
                manifest = (
                    await outputs.finalize(
                        summary=summary, observations=observations.measurements()
                    )
                    if summary.status == "completed"
                    else None
                )
                await self.control.finish_cleanup(lease=state.lease, manifest=manifest)
        finally:
            await http.close()

    async def run_once(self, *, shutdown: asyncio.Event | None = None) -> bool:
        """Handle one new execution or recovery cleanup without reclaiming unknown real work."""
        shutdown = shutdown or asyncio.Event()
        if shutdown.is_set():
            return False
        started = time.monotonic()
        lease = await self.control.claim(owner=self.owner)
        if lease is None:
            return False
        state = Attempt(lease=lease, deadline=started + 25)
        done = asyncio.Event()
        watcher = asyncio.create_task(coro=self._watch(state=state, done=done, shutdown=shutdown))
        self.last_error_code, self.last_failure = None, None
        try:
            if lease.phase == "cleanup":
                await self._cleanup(state=state)
                await self.control.finish_cleanup(lease=state.lease)
            else:
                await self._execute(state=state)
        except FAILURES as error:
            self.last_failure = (
                f"{type(error).__name__}:{error.code}"
                if isinstance(error, (StoreError, OperationError, EngineError, TrafficError))
                else type(error).__name__
            )
            self.last_error_code = (
                "RESOURCE_PRESSURE"
                if isinstance(error, (StoreError, TrafficError))
                and error.code == "RESOURCE_PRESSURE"
                else state.failure
            )
            if state.owns and state.lease.phase == "running":
                try:
                    await self._begin(
                        state=state, outcome="failed", error_code=self.last_error_code
                    )
                    await self._cleanup(state=state)
                    finished = await self.control.finish_cleanup(lease=state.lease)
                    if finished.status == "cancelled":
                        self.last_error_code, self.last_failure = None, None
                except FAILURES:
                    # Keep the reservation and expired-cleanup retry budget as the authority.
                    self.last_error_code = "INFRASTRUCTURE_FAILURE"
        finally:
            done.set()
            await watcher
        return True

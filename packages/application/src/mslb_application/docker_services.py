"""Transport-neutral Docker submission, isolated reads and verified streamed downloads."""

import hashlib
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_artifacts import HttpRecord, RuntimeRecord
from mslb_contracts.docker_runs import DockerSettings, DockerSnapshot
from mslb_contracts.docker_views import (
    DockerArtifactName,
    DockerArtifactView,
    DockerRunDetail,
    DockerRunView,
    DockerSimulationCreate,
)
from mslb_contracts.http import SimulationAccepted
from mslb_contracts.pagination import Page
from mslb_persistence.catalog import Catalog
from mslb_persistence.docker_catalog import DockerCatalog
from mslb_persistence.docker_control import DockerControl
from mslb_persistence.docker_reads import DockerRead, DockerReads
from mslb_persistence.records import DatasetRecord
from mslb_persistence.resource_evidence import ResourceEvidenceStore

from mslb_application.admission import MaterializationAdmission
from mslb_application.cursors import Position
from mslb_application.docker_inputs import DockerSubmissions, load_docker_snapshot
from mslb_application.docker_outputs import verify_docker_outputs
from mslb_application.download import Download
from mslb_application.errors import OperationError
from mslb_application.managed_files import ManagedFiles
from mslb_application.queries import Queries
from mslb_application.storage_encoding import request_bytes
from mslb_application.storage_values import FileReference


class DockerServices:
    """Reuse shared admission/cursors and keep operational artifacts outside logical resolvers."""

    def __init__(
        self,
        *,
        catalog: Catalog,
        inputs: ManagedFiles,
        queries: Queries,
        admission: MaterializationAdmission,
    ) -> None:
        self.catalog, self.inputs, self.queries = catalog, inputs, queries
        self.docker = DockerCatalog(database=catalog.database)
        self.reads = DockerReads(database=catalog.database)
        self.control = DockerControl(database=catalog.database)
        self.submissions = DockerSubmissions(
            catalog=catalog, docker=self.docker, files=inputs, admission=admission
        )
        self.evidence = ResourceEvidenceStore(database=catalog.database)

    async def submit(
        self, *, body: DockerSimulationCreate, key: str | None = None
    ) -> SimulationAccepted:
        """Freeze only operational settings; the server resolves enrolled backend identity."""
        settings = DockerSettings.model_validate(obj=body.model_dump(exclude={"dataset_id"}))
        run = await self.submissions.submit(dataset_id=body.dataset_id, settings=settings, key=key)
        saved = await self.reads.run(run_id=run.id)
        return SimulationAccepted(
            run_id=run.id, status=saved.run.status, snapshot_digest=run.snapshot_digest
        )

    async def history(self, *, limit: int = 100, cursor: str | None = None) -> Page[DockerRunView]:
        """Page only Docker runs, preserving the common signed byte-bounded cursor policy."""
        point = self.queries.cursors.decode(context="docker-runs", token=cursor)
        if (
            point.key is not None
            or point.sequence
            or ((point.id is None) != (point.created_at is None))
        ):
            raise OperationError(code="VALIDATION_ERROR", message="Invalid Docker history position")
        rows = await self.reads.runs(
            after=(point.created_at, point.id)
            if point.created_at is not None and point.id is not None
            else None,
            limit=limit,
        )
        return self.queries._page(
            rows=rows,
            limit=limit,
            context="docker-runs",
            position=lambda run: Position(created_at=run.created_at, id=run.id),
        )

    def _result(
        self, *, saved: DockerRead, snapshot: DockerSnapshot, dataset: DatasetRecord
    ) -> tuple[RuntimeRecord, tuple[DockerArtifactView, ...]]:
        """Verify final scope, inputs, lifecycle and HTTP accounting before exposing success."""
        manifest = saved.manifest
        if manifest is None:
            raise OperationError(code="INVALID_STATE", message="Docker result is not complete")
        managed = load_docker_snapshot(
            files=self.inputs,
            key=f"docker/snapshots/{snapshot.digest()}.json",
            expected_digest=snapshot.digest(),
        )
        if managed != snapshot or snapshot.dataset_digest != dataset.digest:
            raise OperationError(code="VALIDATION_FAILED", message="Docker input identities differ")
        requests = self.inputs.load_requests(digest=dataset.digest, size_bytes=dataset.size_bytes)
        runtime = verify_docker_outputs(
            files=self.queries.outputs, manifest=manifest, snapshot=snapshot, requests=requests
        )
        csv_hash = hashlib.sha256()
        for block in request_bytes(requests=requests):
            csv_hash.update(block)
        artifacts = (
            *(
                DockerArtifactView.model_validate_json(
                    json_data=canonical_bytes(
                        value={
                            "name": name,
                            "sha256": reference.sha256,
                            "size_bytes": reference.size_bytes,
                        }
                    )
                )
                for name, reference in sorted(manifest.artifacts.items())
            ),
            DockerArtifactView(
                name="snapshot.json",
                sha256=snapshot.digest(),
                size_bytes=len(canonical_bytes(value=snapshot.model_dump(mode="json"))),
            ),
            DockerArtifactView(
                name="requests.csv", sha256=csv_hash.hexdigest(), size_bytes=dataset.size_bytes
            ),
        )
        return runtime, artifacts

    async def _completed(
        self, *, saved: DockerRead
    ) -> tuple[RuntimeRecord, tuple[DockerArtifactView, ...]]:
        if saved.manifest is None:
            raise OperationError(code="INVALID_STATE", message="Docker result is not complete")
        snapshot = await self.docker.snapshot(run_id=saved.run.id)
        if snapshot.digest() != saved.run.snapshot_digest:
            raise OperationError(
                code="VALIDATION_FAILED", message="Docker snapshot identity differs"
            )
        dataset = await self.catalog.get_dataset(dataset_id=saved.run.dataset_id)
        return await self.queries._file_call(
            operation=lambda: self._result(saved=saved, snapshot=snapshot, dataset=dataset)
        )

    async def detail(self, *, run_id: UUID) -> DockerRunDetail:
        """Return durable lifecycle immediately and only independently verified final results."""
        saved = await self.reads.run(run_id=run_id)
        report = await self.evidence.read()
        detail = DockerRunDetail(
            run=saved.run,
            cleanup_pending=saved.cleanup_pending,
            resources=saved.resources,
            admission=report.decision,
        )
        if saved.manifest is not None:
            runtime, artifacts = await self._completed(saved=saved)
            detail = detail.model_copy(update={"summary": runtime.traffic, "artifacts": artifacts})
        return self.queries._bounded(value=detail)

    async def cancel(self, *, run_id: UUID) -> DockerRunDetail:
        """Request mode-checked durable cancellation while cleanup retains its ownership."""
        await self.control.cancel(run_id=run_id)
        return await self.detail(run_id=run_id)

    async def download(self, *, run_id: UUID, name: DockerArtifactName) -> Download:
        """Resolve only a typed matching Docker artifact or frozen input from a completed run."""
        for download in await self.downloads(run_id=run_id):
            if download.filename == name:
                return download
        raise OperationError(code="NOT_FOUND", message="Docker artifact does not exist")

    async def downloads(self, *, run_id: UUID) -> tuple[Download, ...]:
        """Verify the matching package once before constructing its bounded stream handles."""
        saved = await self.reads.run(run_id=run_id)
        _runtime, artifacts = await self._completed(saved=saved)
        return tuple(self._download(saved=saved, metadata=metadata) for metadata in artifacts)

    def _download(self, *, saved: DockerRead, metadata: DockerArtifactView) -> Download:
        if saved.manifest is None:
            raise OperationError(code="NOT_FOUND", message="Docker artifact does not exist")
        name = metadata.name
        if name == "requests.csv":
            files, key = self.inputs, f"datasets/{saved.run.dataset_digest}.csv"
        elif name == "snapshot.json":
            files, key = self.inputs, f"docker/snapshots/{saved.run.snapshot_digest}.json"
        else:
            files, key = self.queries.outputs, saved.manifest.artifacts[name].key
        return Download(
            files=files,
            reference=FileReference(
                key=key, sha256=metadata.sha256, size_bytes=metadata.size_bytes
            ),
            filename=name,
            media_type="text/csv"
            if name.endswith(".csv")
            else "application/x-ndjson"
            if name.endswith(".jsonl")
            else "application/json",
        )

    def _events(self, *, saved: DockerRead, after: int, limit: int) -> tuple[HttpRecord, ...]:
        if saved.manifest is None:
            raise OperationError(code="INVALID_STATE", message="Docker events are not complete")
        rows: list[HttpRecord] = []
        reference = FileReference(**saved.manifest.artifacts["http_events.jsonl"].model_dump())
        with self.queries.outputs.verified_stream(reference=reference) as stream:
            while line := stream.readline(16 * 1024 + 1):
                if len(line) > 16 * 1024:
                    raise OperationError(
                        code="VALIDATION_FAILED", message="Docker event exceeds bounds"
                    )
                event = HttpRecord.model_validate_json(json_data=line)
                if event.sequence > after:
                    rows.append(event)
                if len(rows) >= limit:
                    break
        return tuple(rows)

    async def events(
        self, *, run_id: UUID, limit: int = 100, cursor: str | None = None
    ) -> Page[HttpRecord]:
        """Page verified completed real-time events using run-bound signed continuations."""
        if type(limit) is not int or not 1 <= limit <= 500:
            raise OperationError(code="VALIDATION_ERROR", message="Invalid Docker event page size")
        saved = await self.reads.run(run_id=run_id)
        await self._completed(saved=saved)
        context = ("docker-events", str(object=run_id), saved.run.snapshot_digest)
        point = self.queries.cursors.decode(context=context, token=cursor)
        if point.id is not None or point.created_at is not None or point.key is not None:
            raise OperationError(code="VALIDATION_ERROR", message="Invalid Docker event position")
        rows = await self.queries._file_call(
            operation=lambda: self._events(saved=saved, after=point.sequence, limit=limit)
        )
        return self.queries._page(
            rows=rows,
            limit=limit,
            context=context,
            position=lambda event: Position(sequence=event.sequence),
        )

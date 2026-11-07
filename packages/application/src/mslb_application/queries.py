"""Shared run history, persisted events, verified downloads, charts, and comparison."""

import asyncio
import hashlib
from collections.abc import Callable
from typing import Literal
from uuid import UUID

from ijson.common import JSONError
from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.capacity_profiles import STRESS_REQUESTS
from mslb_contracts.durability import PersistedEvent
from mslb_contracts.history import HistoryEntry, HistoryFilters, HistoryKind
from mslb_contracts.playback import PlaybackView
from mslb_contracts.queries import (
    ChartView,
    ComparisonRun,
    ComparisonView,
    DatasetView,
    JsonValue,
    MetricsView,
    Page,
    RunView,
    ServerMetricView,
)
from mslb_contracts.runs import BundleManifest
from mslb_engine.metrics import chart_buckets
from mslb_persistence.database import Database
from mslb_persistence.durability import Durability
from mslb_persistence.history_reads import HistoryReads
from mslb_persistence.reads import Reads, check_limit
from pydantic import BaseModel

from mslb_application.artifact_validation import ARTIFACT_NAMES
from mslb_application.cursors import Cursors, Position, history_position
from mslb_application.download import Download as Download
from mslb_application.errors import OperationError
from mslb_application.managed_files import ManagedFiles
from mslb_application.metric_queries import (
    chart_samples,
    compare_reports,
    metric_report,
    server_metric_page,
)
from mslb_application.playback import read_playback
from mslb_application.storage_values import FileReference

type ArtifactName = Literal[
    "run.jsonl",
    "autoscaling_decisions.jsonl",
    "drop_reasons.jsonl",
    "timeline.jsonl",
    "metrics.json",
    "snapshot.json",
    "servers.json",
    "requests.csv",
    "manifest.json",
]


type ChartMetric = Literal[
    "cpu_utilization", "memory_utilization", "pending_depth", "active_requests"
]


class Queries:
    """Read committed history and verified artifacts within response and concurrency budgets."""

    def __init__(
        self,
        database: Database,
        outputs: ManagedFiles,
        cursor_secret: bytes,
        response_bytes: int = 1024**2,
    ) -> None:
        if type(response_bytes) is not int or not 1024 <= response_bytes <= 1024**2:
            raise OperationError(
                code="VALIDATION_ERROR", message="Response budget must be 1 KiB–1 MiB"
            )
        self.reads, self.durable = Reads(database=database), Durability(database=database)
        self.outputs, self.cursors = outputs, Cursors(secret=cursor_secret)
        self.response_bytes = response_bytes
        self.file_queries = asyncio.Semaphore(value=1)

    def _bounded[T: BaseModel](self, value: T) -> T:
        if len(value.model_dump_json().encode()) > self.response_bytes:
            raise OperationError(
                code="RESOURCE_LIMIT_EXCEEDED", message="Query exceeds encoded response budget"
            )
        return value

    async def _file_call[T](self, operation: Callable[[], T]) -> T:
        """Retain the bounded file-query slot until a cancelled thread operation settles."""
        async with self.file_queries:
            task = asyncio.create_task(coro=asyncio.to_thread(operation))
            try:
                return await asyncio.shield(arg=task)
            except asyncio.CancelledError:
                # A cancelled HTTP request cannot release admission while its
                # bounded disk/parser thread is still running.
                drained = asyncio.gather(task, return_exceptions=True)
                while not drained.done():
                    try:
                        await asyncio.shield(arg=drained)
                    except asyncio.CancelledError:
                        continue
                raise
            except OperationError:
                raise
            except (ValueError, OSError, JSONError):
                raise OperationError(
                    code="VALIDATION_FAILED",
                    message="Stored query artifact is unavailable or invalid",
                ) from None

    def _page[T: BaseModel](
        self, rows: tuple[T, ...], limit: int, context: object, position: Callable[[T], Position]
    ) -> Page[T]:
        """Fit an encoded page budget without skipping the first unreturned item."""
        items: list[T] = []
        for row in rows:
            trial = Page[T](
                items=(*items, row),
                next_cursor=self.cursors.encode(context=context, position=position(row)),
            )
            if len(trial.model_dump_json().encode()) > self.response_bytes:
                if not items:
                    raise OperationError(
                        code="RESOURCE_LIMIT_EXCEEDED", message="One item exceeds response budget"
                    )
                break
            items.append(row)
        more = len(items) < len(rows) or len(rows) == limit
        cursor = (
            self.cursors.encode(context=context, position=position(items[-1]))
            if items and more
            else None
        )
        return self._bounded(value=Page[T](items=tuple(items), next_cursor=cursor))

    async def runs(self, limit: int = 100, cursor: str | None = None) -> Page[RunView]:
        """Page logical run summaries through signed, query-bound cursors."""
        check_limit(limit=limit)
        point = self.cursors.decode(context="runs", token=cursor)
        after = history_position(point=point)
        rows = await self.reads.runs(after=after, limit=limit)
        return self._page(
            rows=rows,
            limit=limit,
            context="runs",
            position=lambda row: Position(created_at=row.created_at, id=row.id),
        )

    async def history(
        self,
        *,
        kind: HistoryKind,
        filters: HistoryFilters,
        limit: int = 20,
        cursor: str | None = None,
    ) -> Page[HistoryEntry]:
        """Bind complete filters and newest-first ordering into continuation identity."""
        check_limit(limit=limit)
        context = {
            "collection": "browser-history-v1",
            "kind": kind,
            "filters": filters.model_dump(mode="json"),
        }
        point = self.cursors.decode(context=context, token=cursor)
        rows = await HistoryReads(database=self.reads.database).page(
            kind=kind, filters=filters, before=history_position(point=point), limit=limit
        )
        page = self._page(
            rows=rows[:limit],
            limit=limit,
            context=context,
            position=lambda row: Position(created_at=row.created_at, id=row.id),
        )
        if len(rows) <= limit and len(page.items) == len(rows):
            return page.model_copy(update={"next_cursor": None})
        return page

    async def datasets(self, limit: int = 100, cursor: str | None = None) -> Page[DatasetView]:
        """Page dataset metadata without materializing stored CSV contents."""
        check_limit(limit=limit)
        point = self.cursors.decode(context="datasets", token=cursor)
        rows = await self.reads.datasets(after=history_position(point=point), limit=limit)
        return self._page(
            rows=rows,
            limit=limit,
            context="datasets",
            position=lambda row: Position(created_at=row.created_at, id=row.id),
        )

    async def run(self, run_id: UUID) -> RunView:
        """Read one logical run's durable status and committed progress."""
        return self._bounded(value=await self.reads.run(run_id=run_id))

    async def dataset(self, dataset_id: UUID) -> DatasetView:
        """Read one public dataset view by its validated identifier."""
        return self._bounded(value=await self.reads.dataset(dataset_id=dataset_id))

    async def events(
        self,
        run_id: UUID,
        limit: int = 100,
        cursor: str | None = None,
        start_tick: int = 0,
        end_tick: int = 1_000_000,
    ) -> Page[PersistedEvent]:
        """Read committed ascending events with cursor-bound inclusive tick filters."""
        check_limit(limit=limit)
        run = await self.reads.run(run_id=run_id)
        context = ["events", str(object=run_id), run.snapshot_digest, start_tick, end_tick]
        point = self.cursors.decode(context=context, token=cursor)
        if point.id is not None or point.created_at is not None or point.key is not None:
            raise OperationError(
                code="VALIDATION_ERROR", message="Cursor does not contain an event position"
            )
        rows = await self.durable.events(
            run_id=run_id,
            after_sequence=point.sequence,
            limit=limit,
            start_tick=start_tick,
            end_tick=end_tick,
        )
        return self._page(
            rows=rows,
            limit=limit,
            context=context,
            position=lambda row: Position(sequence=row.sequence),
        )

    def _manifest(self, run: RunView, pointer: FileReference) -> BundleManifest:
        """Require a completed run and its verified published manifest."""
        if not pointer.key.startswith(f"runs/{run.id}/attempt-") or pointer.size_bytes > 1024**2:
            raise OperationError(
                code="VALIDATION_FAILED", message="Published manifest reference is invalid"
            )
        with self.outputs.verified_stream(reference=pointer) as stream:
            payload = stream.read(1024**2 + 1)
        manifest = BundleManifest.model_validate_json(json_data=payload)
        digest = hashlib.sha256(
            data=canonical_bytes(value=manifest.model_dump(mode="json", exclude={"bundle_digest"}))
        ).hexdigest()
        if (
            digest != manifest.bundle_digest
            or set(manifest.artifacts) != ARTIFACT_NAMES
            or manifest.snapshot_digest != run.snapshot_digest
            or manifest.engine_version != run.engine_version
            or not pointer.key.endswith(f"/bundles/{digest}/manifest.json")
        ):
            raise OperationError(
                code="VALIDATION_FAILED", message="Published manifest identity differs"
            )
        return manifest

    async def completed(self, run_id: UUID) -> tuple[RunView, BundleManifest, FileReference]:
        """Require a completed, published logical run before serving result artifacts."""
        run = await self.reads.run(run_id=run_id)
        if run.status != "completed":
            raise OperationError(
                code="INVALID_STATE", message="Final results require a completed run"
            )
        pointer = await self.durable.published(run_id=run_id)
        reference = FileReference(
            key=pointer.key, sha256=pointer.sha256, size_bytes=pointer.size_bytes
        )
        manifest = await self._file_call(
            operation=lambda: self._manifest(run=run, pointer=reference)
        )
        return run, manifest, reference

    async def download(self, run_id: UUID, name: str = "run.jsonl") -> Download:
        """Authorize a named artifact through its committed manifest and exact reference."""
        if name not in ARTIFACT_NAMES | {"manifest.json"}:
            raise OperationError(code="VALIDATION_ERROR", message="Unknown artifact type")
        _, manifest, pointer = await self.completed(run_id=run_id)
        reference = pointer
        if name != "manifest.json":
            metadata = manifest.artifacts[name]
            reference = FileReference(
                key=pointer.key.rsplit(sep="/", maxsplit=1)[0] + "/" + name,
                sha256=metadata.sha256,
                size_bytes=metadata.size_bytes,
            )
        media = (
            "text/csv"
            if name.endswith(".csv")
            else "application/x-ndjson"
            if name.endswith(".jsonl")
            else "application/json"
        )
        return Download(files=self.outputs, reference=reference, filename=name, media_type=media)

    def _metrics(
        self, download: Download, server_ids: tuple[str, ...]
    ) -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
        with download.open() as stream:
            return metric_report(stream=stream, server_ids=server_ids)

    async def metrics(self, run_id: UUID, server_ids: tuple[str, ...] = ()) -> MetricsView:
        """Read exact persisted summary metrics within the shared response budget."""
        if (
            type(server_ids) is not tuple
            or len(server_ids) > 32
            or len(set(server_ids)) != len(server_ids)
            or any(type(sid) is not str or not 1 <= len(sid.encode()) <= 128 for sid in server_ids)
        ):
            raise OperationError(
                code="VALIDATION_ERROR", message="Select at most 32 distinct metric servers"
            )
        _, manifest, _ = await self.completed(run_id=run_id)
        download = await self.download(run_id=run_id, name="metrics.json")
        summary, servers = await self._file_call(
            operation=lambda: self._metrics(download=download, server_ids=server_ids)
        )
        return self._bounded(
            value=MetricsView(
                run_id=run_id,
                bundle_digest=manifest.bundle_digest,
                summary=summary,
                servers=servers,
            )
        )

    def _chart(
        self,
        download: Download,
        metric: str,
        server_id: str | None,
        start: int,
        end: int,
        max_points: int,
    ) -> tuple[dict[str, JsonValue], ...]:
        with download.open() as stream:
            return tuple(
                chart_buckets(
                    samples=chart_samples(stream=stream, metric=metric, server_id=server_id),
                    start=start,
                    end=end,
                    max_points=max_points,
                )
            )

    async def chart(
        self,
        run_id: UUID,
        metric: ChartMetric,
        start_tick: int,
        end_tick: int,
        max_points: int = 2000,
        server_id: str | None = None,
    ) -> ChartView:
        """Query bounded persisted buckets without executing the engine."""
        if (
            metric
            not in {"cpu_utilization", "memory_utilization", "pending_depth", "active_requests"}
            or any(type(value) is not int for value in (start_tick, end_tick, max_points))
            or not 0 <= start_tick < end_tick <= 1_000_000
            or not 1 <= max_points <= 2000
            or (
                server_id is not None
                and (
                    type(server_id) is not str
                    or not 1 <= len(server_id.encode()) <= 128
                    or metric == "pending_depth"
                )
            )
        ):
            raise OperationError(code="VALIDATION_ERROR", message="Invalid chart query")
        _, manifest, _ = await self.completed(run_id=run_id)
        download = await self.download(run_id=run_id, name="timeline.jsonl")
        points = await self._file_call(
            operation=lambda: self._chart(
                download=download,
                metric=metric,
                server_id=server_id,
                start=start_tick,
                end=end_tick,
                max_points=max_points,
            )
        )
        return self._bounded(
            value=ChartView(
                run_id=run_id,
                bundle_digest=manifest.bundle_digest,
                metric=metric,
                server_id=server_id,
                start_tick=start_tick,
                end_tick=end_tick,
                points=points,
            )
        )

    async def server_metrics(
        self, run_id: UUID, limit: int = 100, cursor: str | None = None
    ) -> Page[ServerMetricView]:
        """Page exact saved server metrics with a filter-bound cursor."""
        check_limit(limit=limit)
        _, manifest, _ = await self.completed(run_id=run_id)
        context = ["server_metrics", str(object=run_id), manifest.bundle_digest]
        point = self.cursors.decode(context=context, token=cursor)
        if point.id is not None or point.created_at is not None or point.sequence:
            raise OperationError(
                code="VALIDATION_ERROR", message="Cursor does not contain a server position"
            )
        download = await self.download(run_id=run_id, name="metrics.json")

        def read() -> tuple[ServerMetricView, ...]:
            with download.open() as stream:
                return server_metric_page(stream=stream, after=point.key, limit=limit)

        rows = await self._file_call(operation=read)
        return self._page(
            rows=rows, limit=limit, context=context, position=lambda row: Position(key=row.id)
        )

    async def playback(
        self,
        run_id: UUID,
        tick: int,
        server_offset: int = 0,
        request_kind: Literal["pending", "active"] = "pending",
        server_id: str | None = None,
        request_offset: int = 0,
    ) -> PlaybackView:
        """Read bounded saved membership/request slices at a logical tick."""
        if (
            any(type(value) is not int for value in (tick, server_offset, request_offset))
            or not 0 <= tick <= 1_000_000
            or not 0 <= server_offset <= 64
            or not 0 <= request_offset <= STRESS_REQUESTS
            or request_kind not in {"pending", "active"}
            or (request_kind == "active") != (server_id is not None)
            or (
                server_id is not None
                and (type(server_id) is not str or not 1 <= len(server_id.encode()) <= 128)
            )
        ):
            raise OperationError(code="VALIDATION_ERROR", message="Invalid playback query")
        _, manifest, _ = await self.completed(run_id=run_id)
        if tick > manifest.terminal_tick:
            raise OperationError(code="NOT_FOUND", message="Tick is outside the completed run")
        timeline = await self.download(run_id=run_id, name="timeline.jsonl")
        scaling = await self.download(run_id=run_id, name="autoscaling_decisions.jsonl")

        def read() -> PlaybackView:
            with timeline.open() as intervals, scaling.open() as decisions:
                return read_playback(
                    timeline=intervals,
                    scaling=decisions,
                    run_id=run_id,
                    bundle_digest=manifest.bundle_digest,
                    tick=tick,
                    server_offset=server_offset,
                    request_kind=request_kind,
                    server_id=server_id,
                    request_offset=request_offset,
                )

        return self._bounded(value=await self._file_call(operation=read))

    async def compare(self, run_ids: tuple[UUID, ...]) -> ComparisonView:
        """Reject incompatible identities before comparing completed logical results."""
        if not 2 <= len(run_ids) <= 8 or len(set(run_ids)) != len(run_ids):
            raise OperationError(
                code="VALIDATION_ERROR", message="Compare 2–8 distinct completed logical runs"
            )
        baseline = await self.reads.run(run_id=run_ids[0])
        first = await self.metrics(run_id=run_ids[0])
        results: list[ComparisonRun] = []
        for run_id in run_ids:
            run = await self.reads.run(run_id=run_id)
            report = first if run_id == baseline.id else await self.metrics(run_id=run_id)
            differences = tuple(
                name
                for name in ("strategy", "autoscaling")
                if getattr(run.configuration, name) != getattr(baseline.configuration, name)
            )
            values = compare_reports(
                baseline=baseline, candidate=run, first=first.summary, second=report.summary
            )
            results.append(
                ComparisonRun(
                    run_id=run_id,
                    bundle_digest=report.bundle_digest,
                    configuration=run.configuration,
                    differing_settings=differences,
                    metrics=values,
                )
            )
        return self._bounded(value=ComparisonView(baseline_run_id=baseline.id, runs=tuple(results)))

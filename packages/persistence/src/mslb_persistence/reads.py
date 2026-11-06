"""Coherent bounded read transactions for application adapters."""

from datetime import datetime
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.queries import DatasetView, RunProgress, RunView
from mslb_contracts.runs import LegacyRunSettings, public_settings
from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from mslb_persistence.database import Database, StoreError
from mslb_persistence.models import Dataset, Job, OutputProgress, Run


def dataset_view(row: Dataset) -> DatasetView:
    return DatasetView(
        id=row.id,
        digest=row.digest,
        source_filename=row.source_filename,
        request_count=row.request_count,
        earliest_arrival=row.earliest_arrival,
        latest_arrival=row.latest_arrival,
        size_bytes=row.size_bytes,
        created_at=row.created_at,
        original_digest=row.original_digest,
        source_size_bytes=row.source_size_bytes,
    )


def run_view(run: Run, job: Job, progress: OutputProgress | None) -> RunView:
    # JSON validation is explicit at the ORM boundary; no live entity or file key escapes.
    configuration = run.configuration
    if configuration.get("mode") == "sequential_compat":
        configuration = public_settings(
            settings=LegacyRunSettings.model_validate(obj=configuration)
        ).model_dump(mode="json")
    data = dict(
        id=str(run.id),
        dataset_id=str(run.dataset_id),
        snapshot_digest=run.snapshot_digest,
        dataset_digest=run.dataset_digest,
        server_snapshot_digest=run.server_snapshot_digest,
        engine_version=run.engine_version,
        configuration=configuration,
        status=job.status,
        attempts=job.attempts,
        created_at=run.created_at.isoformat(),
        completed_at=job.completed_at.isoformat() if job.completed_at else None,
        error_code=job.error_code,
        progress=None,
    )
    view = RunView.model_validate_json(canonical_bytes(data))
    if progress is None:
        return view
    counts = [p.get("records") for p in progress.prefixes if p.get("name") == "run.jsonl"]
    if len(counts) != 1 or type(counts[0]) is not int:
        raise StoreError("VALIDATION_FAILED", "Committed progress metadata is invalid")
    return view.model_copy(
        update={
            "progress": RunProgress.model_validate_json(
                canonical_bytes(
                    dict(
                        revision=progress.revision,
                        kind=progress.kind,
                        next_tick=progress.next_tick,
                        executed_intervals=progress.executed_intervals,
                        committed_events=counts[0],
                    )
                )
            )
        }
    )


def check_limit(limit: int) -> None:
    if type(limit) is not int or not 1 <= limit <= 500:
        raise StoreError("VALIDATION_ERROR", "Page limit must be between 1 and 500")


class Reads:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def run(self, run_id: UUID) -> RunView:
        async def operation(session: AsyncSession) -> RunView:
            row = (
                await session.execute(
                    select(Run, Job, OutputProgress)
                    .join(Job, Job.run_id == Run.id)
                    .outerjoin(OutputProgress, OutputProgress.run_id == Run.id)
                    .where(Run.id == run_id, Job.kind == "logical")
                )
            ).one_or_none()
            if row is None:
                raise StoreError("NOT_FOUND", "Run does not exist")
            return run_view(*row)

        return await self.database.transaction(operation)

    async def runs(
        self, after: tuple[datetime, UUID] | None = None, limit: int = 100
    ) -> tuple[RunView, ...]:
        check_limit(limit)

        async def operation(session: AsyncSession) -> tuple[RunView, ...]:
            query = (
                select(Run, Job, OutputProgress)
                .join(Job, Job.run_id == Run.id)
                .outerjoin(OutputProgress, OutputProgress.run_id == Run.id)
                .where(Job.kind == "logical")
            )
            if after is not None:
                query = query.where(tuple_(Run.created_at, Run.id) > after)
            rows = await session.execute(query.order_by(Run.created_at, Run.id).limit(limit))
            return tuple(run_view(*row) for row in rows)

        return await self.database.transaction(operation)

    async def dataset(self, dataset_id: UUID) -> DatasetView:
        async def operation(session: AsyncSession) -> DatasetView:
            row = await session.get(Dataset, dataset_id)
            if row is None:
                raise StoreError("NOT_FOUND", "Dataset does not exist")
            return dataset_view(row)

        return await self.database.transaction(operation)

    async def datasets(
        self, after: tuple[datetime, UUID] | None = None, limit: int = 100
    ) -> tuple[DatasetView, ...]:
        check_limit(limit)

        async def operation(session: AsyncSession) -> tuple[DatasetView, ...]:
            query = select(Dataset)
            if after is not None:
                query = query.where(tuple_(Dataset.created_at, Dataset.id) > after)
            rows = await session.scalars(
                query.order_by(Dataset.created_at, Dataset.id).limit(limit)
            )
            return tuple(dataset_view(row) for row in rows)

        return await self.database.transaction(operation)

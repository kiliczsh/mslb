"""Join dataset summaries and durable status before newest-first cursor pagination."""

from datetime import datetime
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.history import HistoryEntry, HistoryFilters, HistoryKind
from sqlalchemy import String, cast, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from mslb_persistence.catalog_models import Dataset
from mslb_persistence.database import Database
from mslb_persistence.job_models import Job, Run
from mslb_persistence.output_models import OutputProgress
from mslb_persistence.reads import check_limit, run_view


class HistoryReads:
    """Keep filtering, ordering and metadata retrieval in one bounded SQL query."""

    def __init__(self, *, database: Database) -> None:
        self.database = database

    async def page(
        self,
        *,
        kind: HistoryKind,
        filters: HistoryFilters,
        before: tuple[datetime, UUID] | None = None,
        limit: int = 20,
    ) -> tuple[HistoryEntry, ...]:
        """Return newest matching rows plus one lookahead for exact continuation."""
        check_limit(limit=limit)

        async def operation(session: AsyncSession) -> tuple[HistoryEntry, ...]:
            query = (
                select(Run, Job, OutputProgress, Dataset)
                .join(target=Job, onclause=Job.run_id == Run.id)
                .join(target=Dataset, onclause=Dataset.id == Run.dataset_id)
                .outerjoin(target=OutputProgress, onclause=OutputProgress.run_id == Run.id)
                .where(Job.kind == kind)
            )
            if before is not None:
                query = query.where(tuple_(Run.created_at, Run.id) < before)
            if filters.search:
                query = query.where(
                    or_(
                        Dataset.source_filename.icontains(other=filters.search, autoescape=True),
                        cast(expression=Run.id, type_=String).icontains(
                            other=filters.search, autoescape=True
                        ),
                    )
                )
            if filters.status == "active":
                query = query.where(Job.status.in_(other=("queued", "running", "cancel_requested")))
            elif filters.status is not None:
                query = query.where(Job.status == filters.status)
            if filters.strategy is not None:
                query = query.where(Run.configuration["strategy"].as_string() == filters.strategy)
            if filters.mode == "sequential":
                query = query.where(
                    Run.configuration["mode"]
                    .as_string()
                    .in_(other=("sequential", "sequential_compat"))
                )
            elif filters.mode is not None:
                query = query.where(Run.configuration["mode"].as_string() == filters.mode)
            rows = await session.execute(
                statement=query.order_by(Run.created_at.desc(), Run.id.desc()).limit(
                    limit=limit + 1
                )
            )
            result = []
            for run, job, progress, dataset in rows:
                mode = run.configuration.get("mode", "sequential")
                public_progress = (
                    run_view(run=run, job=job, progress=progress).progress
                    if kind == "logical"
                    else None
                )
                result.append(
                    HistoryEntry.model_validate_json(
                        json_data=canonical_bytes(
                            value={
                                "id": str(object=run.id),
                                "dataset_id": str(object=dataset.id),
                                "source_filename": dataset.source_filename,
                                "request_count": dataset.request_count,
                                "strategy": run.configuration["strategy"],
                                "execution_mode": "sequential"
                                if mode == "sequential_compat"
                                else mode,
                                "status": job.status,
                                "created_at": run.created_at.isoformat(),
                                "completed_at": job.completed_at.isoformat()
                                if job.completed_at
                                else None,
                                "error_code": job.error_code,
                                "progress": public_progress.model_dump(mode="json")
                                if public_progress is not None
                                else None,
                            }
                        )
                    )
                )
            return tuple(result)

        return await self.database.transaction(operation=operation)

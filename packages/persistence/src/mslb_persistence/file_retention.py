"""Bounded coherent reference snapshots for project-only orphan file maintenance."""

from dataclasses import dataclass

from sqlalchemy import or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from mslb_persistence.database import Database
from mslb_persistence.docker_models import DockerControlSlot, DockerPublication
from mslb_persistence.errors import StoreError
from mslb_persistence.job_models import ExecutionSlot, Job
from mslb_persistence.output_models import PublishedArtifact


@dataclass(frozen=True, slots=True, kw_only=True)
class RetentionSnapshot:
    """Protect active work, every published run scope and committed file references."""

    active: frozenset[str]
    published: frozenset[str]
    references: frozenset[str]


class FileRetention:
    """Read a single repeatable snapshot without loading event bodies or manifests."""

    def __init__(self, *, database: Database) -> None:
        self.database = database

    async def snapshot(self) -> RetentionSnapshot:
        """Snapshot all database file references that maintenance must protect from deletion."""

        async def operation(session: AsyncSession) -> RetentionSnapshot:
            active_ids = tuple(
                await session.scalars(
                    statement=select(Job.run_id)
                    .where(
                        or_(
                            Job.status.in_(other=("queued", "running", "cancel_requested")),
                            Job.run_id.in_(
                                other=select(ExecutionSlot.run_id).where(
                                    ExecutionSlot.run_id.is_not(None)
                                )
                            ),
                            Job.run_id.in_(
                                other=select(DockerControlSlot.run_id).where(
                                    DockerControlSlot.run_id.is_not(None)
                                )
                            ),
                        )
                    )
                    .limit(limit=100001)
                )
            )
            published_ids = tuple(
                await session.scalars(
                    statement=select(PublishedArtifact.run_id).limit(limit=100001)
                )
            )
            docker_ids = tuple(
                await session.scalars(
                    statement=select(DockerPublication.run_id).limit(limit=100001)
                )
            )
            keys = tuple(
                await session.scalars(
                    statement=text(
                        text="""
                SELECT found.value #>> '{}' FROM (
                    SELECT reference AS value FROM output_progress
                    UNION ALL SELECT prefixes FROM output_progress
                    UNION ALL SELECT metrics FROM output_progress WHERE metrics IS NOT NULL
                    UNION ALL SELECT reference FROM saved_checkpoints
                    UNION ALL SELECT file FROM committed_chunks
                ) AS documents
                CROSS JOIN LATERAL jsonb_path_query(documents.value,
                    '$.**.key ? (@.type() == "string")') AS found(value)
                LIMIT 100001
            """
                    )
                )
            )
            if max(len(active_ids), len(published_ids) + len(docker_ids), len(keys)) > 100000:
                raise StoreError(
                    code="RESOURCE_LIMIT_EXCEEDED", message="Retention snapshot exceeds its bound"
                )
            if any(not isinstance(key, str) or len(key) > 256 for key in keys):
                raise StoreError(
                    code="VALIDATION_FAILED", message="Stored retention reference is invalid"
                )
            return RetentionSnapshot(
                active=frozenset(
                    scope
                    for run_id in active_ids
                    for scope in (f"runs/{run_id}", f"docker/{run_id.hex}")
                ),
                published=frozenset(
                    scope
                    for run_id in (*published_ids, *docker_ids)
                    for scope in (f"runs/{run_id}", f"docker/{run_id.hex}")
                ),
                references=frozenset(keys),
            )

        return await self.database.transaction(operation=operation)

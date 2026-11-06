"""Relational definitions owned by job."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, MappedColumn

from mslb_persistence.orm_base import Base


class Run(Base):
    """Persist immutable logical input/configuration identities separately from job execution."""

    __tablename__ = "runs"
    id: Mapped[UUID] = MappedColumn[UUID](primary_key=True)
    dataset_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey(column="datasets.id", ondelete="RESTRICT")
    )
    snapshot_digest: Mapped[str] = MappedColumn[str](String(length=64))
    snapshot_key: Mapped[str] = MappedColumn[str](String(length=256))
    dataset_digest: Mapped[str] = MappedColumn[str](String(length=64))
    server_snapshot_digest: Mapped[str] = MappedColumn[str](String(length=64))
    engine_version: Mapped[str] = MappedColumn[str](String(length=64))
    configuration: Mapped[dict[str, object]] = MappedColumn[dict[str, object]](JSONB)
    server_versions: Mapped[dict[str, int]] = MappedColumn[dict[str, int]](JSONB)
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp(), index=True
    )


class Job(Base):
    """Store execution status, bounded attempts and current lease ownership."""

    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(sqltext="kind IN ('logical','docker')", name="kind"),
        CheckConstraint(
            sqltext=(
                "status IN ('queued','running','cancel_requested','completed','failed','cancelled')"
            ),
            name="status",
        ),
        CheckConstraint(sqltext="attempts BETWEEN 0 AND 3 AND fence >= 0", name="attempt_fence"),
        CheckConstraint(
            sqltext="(status IN ('running','cancel_requested') AND owner IS NOT NULL "
            "AND lease_expires_at IS NOT NULL) OR "
            "(status NOT IN ('running','cancel_requested') AND owner IS NULL "
            "AND lease_expires_at IS NULL)",
            name="lease_shape",
        ),
        Index("ix_jobs_claim", "status", "created_at", "run_id"),
        Index("ix_jobs_kind_claim", "kind", "status", "created_at", "run_id"),
    )
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey(column="runs.id", ondelete="RESTRICT"), primary_key=True
    )
    status: Mapped[str] = MappedColumn[str](String(length=32), server_default=text(text="'queued'"))
    kind: Mapped[str] = MappedColumn[str](String(length=16), server_default=text(text="'logical'"))
    attempts: Mapped[int] = MappedColumn[int](Integer, server_default=text(text="0"))
    fence: Mapped[int] = MappedColumn[int](BigInteger, server_default=text(text="0"))
    owner: Mapped[str | None] = MappedColumn[str | None](String(length=128))
    lease_expires_at: Mapped[datetime | None] = MappedColumn[datetime | None](
        DateTime(timezone=True)
    )
    error_code: Mapped[str | None] = MappedColumn[str | None](String(length=64))
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )
    completed_at: Mapped[datetime | None] = MappedColumn[datetime | None](DateTime(timezone=True))


class Attempt(Base):
    """Retain the owner and outcome associated with one monotonically fenced attempt."""

    __tablename__ = "attempts"
    __table_args__ = (UniqueConstraint("run_id", "fence"),)
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey(column="jobs.run_id", ondelete="RESTRICT"), primary_key=True
    )
    number: Mapped[int] = MappedColumn[int](Integer, primary_key=True)
    fence: Mapped[int] = MappedColumn[int](BigInteger)
    owner: Mapped[str] = MappedColumn[str](String(length=128))
    started_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )
    finished_at: Mapped[datetime | None] = MappedColumn[datetime | None](DateTime(timezone=True))
    outcome: Mapped[str | None] = MappedColumn[str | None](String(length=64))


class ExecutionSlot(Base):
    """Admit one active logical job across all worker processes."""

    __tablename__ = "execution_slot"
    __table_args__ = (CheckConstraint(sqltext="id = 1", name="singleton"),)
    id: Mapped[int] = MappedColumn[int](Integer, primary_key=True)
    run_id: Mapped[UUID | None] = MappedColumn[UUID | None](
        ForeignKey(column="jobs.run_id", ondelete="RESTRICT"), unique=True
    )


class Outbox(Base):
    """Persist a durable wake-up hint in the same transaction as submission."""

    __tablename__ = "outbox"
    id: Mapped[UUID] = MappedColumn[UUID](primary_key=True)
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey(column="jobs.run_id", ondelete="RESTRICT"), unique=True
    )
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )
    delivered_at: Mapped[datetime | None] = MappedColumn[datetime | None](
        DateTime(timezone=True), index=True
    )


class IdempotencyRecord(Base):
    """Bind a bounded-lived key and body digest to one durable run."""

    __tablename__ = "idempotency_records"
    scope: Mapped[str] = MappedColumn[str](String(length=128), primary_key=True)
    key_hash: Mapped[str] = MappedColumn[str](String(length=64), primary_key=True)
    body_digest: Mapped[str] = MappedColumn[str](String(length=64))
    run_id: Mapped[UUID] = MappedColumn[UUID](ForeignKey(column="runs.id", ondelete="RESTRICT"))
    expires_at: Mapped[datetime] = MappedColumn[datetime](DateTime(timezone=True), index=True)

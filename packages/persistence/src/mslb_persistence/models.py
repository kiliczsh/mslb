"""Relational ownership and immutable-reference records; no execution logic."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedColumn


class Base(DeclarativeBase):
    metadata = MetaData(
        naming_convention={
            "ix": "ix_%(table_name)s_%(column_0_name)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            "ck": "ck_%(table_name)s_%(constraint_name)s",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s",
        }
    )


class Installation(Base):
    __tablename__ = "installation"
    __table_args__ = (CheckConstraint("id = 1", name="singleton"),)
    id: Mapped[int] = MappedColumn[int](Integer, primary_key=True)
    seeded: Mapped[bool] = MappedColumn[bool](Boolean, server_default=text("false"))
    revision: Mapped[int] = MappedColumn[int](BigInteger, server_default=text("0"))


class ManagedServer(Base):
    __tablename__ = "managed_servers"
    __table_args__ = (
        CheckConstraint("version > 0", name="version_positive"),
        CheckConstraint(
            "cpu_units_per_tick BETWEEN 1 AND 1000000000 "
            "AND mem_mb BETWEEN 1 AND 1000000000 "
            "AND rate_limit_per_sec BETWEEN 1 AND 1000000000",
            name="positive_capacities",
        ),
    )
    id: Mapped[str] = MappedColumn[str](String(128), primary_key=True)
    version: Mapped[int] = MappedColumn[int](Integer)
    cpu_units_per_tick: Mapped[int] = MappedColumn[int](Integer)
    mem_mb: Mapped[int] = MappedColumn[int](Integer)
    rate_limit_per_sec: Mapped[int] = MappedColumn[int](Integer)


class ServerHistory(Base):
    __tablename__ = "server_history"
    server_id: Mapped[str] = MappedColumn[str](String(128), primary_key=True)
    version: Mapped[int] = MappedColumn[int](Integer, primary_key=True)
    deleted: Mapped[bool] = MappedColumn[bool](Boolean)
    cpu_units_per_tick: Mapped[int] = MappedColumn[int](Integer)
    mem_mb: Mapped[int] = MappedColumn[int](Integer)
    rate_limit_per_sec: Mapped[int] = MappedColumn[int](Integer)
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )


class Dataset(Base):
    __tablename__ = "datasets"
    __table_args__ = (
        CheckConstraint(
            "request_count BETWEEN 1 AND 10000 AND size_bytes BETWEEN 1 AND 10485760",
            name="bounded_population",
        ),
        CheckConstraint(
            "earliest_arrival >= 0 AND latest_arrival >= earliest_arrival "
            "AND latest_arrival < 1000000",
            name="arrival_range",
        ),
    )
    id: Mapped[UUID] = MappedColumn[UUID](primary_key=True)
    digest: Mapped[str] = MappedColumn[str](String(64), unique=True)
    storage_key: Mapped[str] = MappedColumn[str](String(256))
    source_filename: Mapped[str] = MappedColumn[str](String(255))
    request_count: Mapped[int] = MappedColumn[int](Integer)
    earliest_arrival: Mapped[int] = MappedColumn[int](Integer)
    latest_arrival: Mapped[int] = MappedColumn[int](Integer)
    size_bytes: Mapped[int] = MappedColumn[int](Integer)
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )


class Run(Base):
    __tablename__ = "runs"
    id: Mapped[UUID] = MappedColumn[UUID](primary_key=True)
    dataset_id: Mapped[UUID] = MappedColumn[UUID](ForeignKey("datasets.id", ondelete="RESTRICT"))
    snapshot_digest: Mapped[str] = MappedColumn[str](String(64))
    snapshot_key: Mapped[str] = MappedColumn[str](String(256))
    dataset_digest: Mapped[str] = MappedColumn[str](String(64))
    server_snapshot_digest: Mapped[str] = MappedColumn[str](String(64))
    engine_version: Mapped[str] = MappedColumn[str](String(64))
    configuration: Mapped[dict[str, object]] = MappedColumn[dict[str, object]](JSONB)
    server_versions: Mapped[dict[str, int]] = MappedColumn[dict[str, int]](JSONB)
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp(), index=True
    )


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('queued','running','cancel_requested','completed','failed','cancelled')",
            name="status",
        ),
        CheckConstraint("attempts BETWEEN 0 AND 3 AND fence >= 0", name="attempt_fence"),
        CheckConstraint(
            "(status IN ('running','cancel_requested') AND owner IS NOT NULL "
            "AND lease_expires_at IS NOT NULL) OR "
            "(status NOT IN ('running','cancel_requested') AND owner IS NULL "
            "AND lease_expires_at IS NULL)",
            name="lease_shape",
        ),
        Index("ix_jobs_claim", "status", "created_at", "run_id"),
    )
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey("runs.id", ondelete="RESTRICT"), primary_key=True
    )
    status: Mapped[str] = MappedColumn[str](String(32), server_default=text("'queued'"))
    attempts: Mapped[int] = MappedColumn[int](Integer, server_default=text("0"))
    fence: Mapped[int] = MappedColumn[int](BigInteger, server_default=text("0"))
    owner: Mapped[str | None] = MappedColumn[str | None](String(128))
    lease_expires_at: Mapped[datetime | None] = MappedColumn[datetime | None](
        DateTime(timezone=True)
    )
    error_code: Mapped[str | None] = MappedColumn[str | None](String(64))
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )
    completed_at: Mapped[datetime | None] = MappedColumn[datetime | None](DateTime(timezone=True))


class Attempt(Base):
    __tablename__ = "attempts"
    __table_args__ = (UniqueConstraint("run_id", "fence"),)
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey("jobs.run_id", ondelete="RESTRICT"), primary_key=True
    )
    number: Mapped[int] = MappedColumn[int](Integer, primary_key=True)
    fence: Mapped[int] = MappedColumn[int](BigInteger)
    owner: Mapped[str] = MappedColumn[str](String(128))
    started_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )
    finished_at: Mapped[datetime | None] = MappedColumn[datetime | None](DateTime(timezone=True))
    outcome: Mapped[str | None] = MappedColumn[str | None](String(64))


class ExecutionSlot(Base):
    __tablename__ = "execution_slot"
    __table_args__ = (CheckConstraint("id = 1", name="singleton"),)
    id: Mapped[int] = MappedColumn[int](Integer, primary_key=True)
    run_id: Mapped[UUID | None] = MappedColumn[UUID | None](
        ForeignKey("jobs.run_id", ondelete="RESTRICT"), unique=True
    )


class Outbox(Base):
    __tablename__ = "outbox"
    id: Mapped[UUID] = MappedColumn[UUID](primary_key=True)
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey("jobs.run_id", ondelete="RESTRICT"), unique=True
    )
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )
    delivered_at: Mapped[datetime | None] = MappedColumn[datetime | None](
        DateTime(timezone=True), index=True
    )


class IdempotencyRecord(Base):
    __tablename__ = "idempotency_records"
    scope: Mapped[str] = MappedColumn[str](String(128), primary_key=True)
    key_hash: Mapped[str] = MappedColumn[str](String(64), primary_key=True)
    body_digest: Mapped[str] = MappedColumn[str](String(64))
    run_id: Mapped[UUID] = MappedColumn[UUID](ForeignKey("runs.id", ondelete="RESTRICT"))
    expires_at: Mapped[datetime] = MappedColumn[datetime](DateTime(timezone=True), index=True)

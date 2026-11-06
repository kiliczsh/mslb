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
        CheckConstraint(
            "(original_digest IS NULL AND source_size_bytes IS NULL) OR "
            "(original_digest IS NOT NULL AND source_size_bytes IS NOT NULL "
            "AND source_size_bytes BETWEEN 1 AND 10485760)",
            name="source_provenance",
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
    original_digest: Mapped[str | None] = MappedColumn[str | None](String(64))
    source_size_bytes: Mapped[int | None] = MappedColumn[int | None](Integer)
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
        CheckConstraint("kind IN ('logical','docker')", name="kind"),
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
        Index("ix_jobs_kind_claim", "kind", "status", "created_at", "run_id"),
    )
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey("runs.id", ondelete="RESTRICT"), primary_key=True
    )
    status: Mapped[str] = MappedColumn[str](String(32), server_default=text("'queued'"))
    kind: Mapped[str] = MappedColumn[str](String(16), server_default=text("'logical'"))
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


class OutputProgress(Base):
    __tablename__ = "output_progress"
    __table_args__ = (
        CheckConstraint("kind IN ('checkpoint','terminal') AND revision > 0", name="kind_revision"),
        CheckConstraint(
            "next_tick BETWEEN 0 AND 1000000 AND executed_intervals BETWEEN 0 AND 1000000",
            name="bounded_progress",
        ),
    )
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey("jobs.run_id", ondelete="RESTRICT"), primary_key=True
    )
    revision: Mapped[int] = MappedColumn[int](Integer)
    kind: Mapped[str] = MappedColumn[str](String(16))
    reference: Mapped[dict[str, object]] = MappedColumn[dict[str, object]](JSONB)
    next_tick: Mapped[int] = MappedColumn[int](Integer)
    executed_intervals: Mapped[int] = MappedColumn[int](Integer)
    prefixes: Mapped[list[dict[str, object]]] = MappedColumn[list[dict[str, object]]](JSONB)
    metrics: Mapped[dict[str, object] | None] = MappedColumn[dict[str, object] | None](
        JSONB, nullable=True
    )


class SavedCheckpoint(Base):
    __tablename__ = "saved_checkpoints"
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey("jobs.run_id", ondelete="RESTRICT"), primary_key=True
    )
    revision: Mapped[int] = MappedColumn[int](Integer, primary_key=True)
    attempt: Mapped[int] = MappedColumn[int](Integer)
    fence: Mapped[int] = MappedColumn[int](BigInteger)
    reference: Mapped[dict[str, object]] = MappedColumn[dict[str, object]](JSONB)
    next_tick: Mapped[int] = MappedColumn[int](Integer)
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )


class CommittedChunk(Base):
    __tablename__ = "committed_chunks"
    __table_args__ = (
        CheckConstraint(
            "first_sequence > 0 AND last_sequence >= first_sequence AND byte_offset >= 0",
            name="contiguous_range",
        ),
    )
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey("jobs.run_id", ondelete="RESTRICT"), primary_key=True
    )
    name: Mapped[str] = MappedColumn[str](String(40), primary_key=True)
    first_sequence: Mapped[int] = MappedColumn[int](Integer, primary_key=True)
    last_sequence: Mapped[int] = MappedColumn[int](Integer)
    byte_offset: Mapped[int] = MappedColumn[int](Integer)
    first_tick: Mapped[int] = MappedColumn[int](Integer)
    last_tick: Mapped[int] = MappedColumn[int](Integer)
    attempt: Mapped[int] = MappedColumn[int](Integer)
    fence: Mapped[int] = MappedColumn[int](BigInteger)
    file: Mapped[dict[str, object]] = MappedColumn[dict[str, object]](JSONB)


class IndexedEvent(Base):
    __tablename__ = "indexed_events"
    __table_args__ = (
        Index("ix_indexed_events_tick", "run_id", "t", "sequence"),
        UniqueConstraint("run_id", "event", "request_id", name="uq_indexed_event_lifecycle"),
    )
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey("jobs.run_id", ondelete="RESTRICT"), primary_key=True
    )
    sequence: Mapped[int] = MappedColumn[int](Integer, primary_key=True)
    t: Mapped[int] = MappedColumn[int](Integer)
    event: Mapped[str] = MappedColumn[str](String(32))
    request_id: Mapped[str] = MappedColumn[str](String(128))
    server_id: Mapped[str | None] = MappedColumn[str | None](String(128))
    reason: Mapped[str | None] = MappedColumn[str | None](String(32))


class PublishedArtifact(Base):
    __tablename__ = "published_artifacts"
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey("jobs.run_id", ondelete="RESTRICT"), primary_key=True
    )
    manifest: Mapped[dict[str, object]] = MappedColumn[dict[str, object]](JSONB)
    bundle_digest: Mapped[str] = MappedColumn[str](String(64))
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )


class DockerDeployment(Base):
    __tablename__ = "docker_deployments"
    id: Mapped[UUID] = MappedColumn[UUID](primary_key=True)
    fingerprint: Mapped[str] = MappedColumn[str](String(64), unique=True)
    definition: Mapped[dict[str, object]] = MappedColumn[dict[str, object]](JSONB)
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )


class DockerRunState(Base):
    __tablename__ = "docker_run_state"
    __table_args__ = (
        CheckConstraint(
            "desired_status IS NULL OR desired_status IN ('completed','failed','cancelled')",
            name="desired_status",
        ),
    )
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey("runs.id", ondelete="RESTRICT"), primary_key=True
    )
    deployment_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey("docker_deployments.id", ondelete="RESTRICT")
    )
    snapshot: Mapped[dict[str, object]] = MappedColumn[dict[str, object]](JSONB)
    desired_status: Mapped[str | None] = MappedColumn[str | None](String(16))
    desired_error: Mapped[str | None] = MappedColumn[str | None](String(64))


class DockerControlSlot(Base):
    __tablename__ = "docker_control_slot"
    __table_args__ = (
        CheckConstraint(
            "id = 1 AND fence >= 0 AND revision >= 0 AND cleanup_attempts BETWEEN 0 AND 3",
            name="singleton_fence",
        ),
        CheckConstraint("phase IN ('idle','running','cleanup')", name="phase"),
        CheckConstraint(
            "(phase = 'idle' AND run_id IS NULL AND owner IS NULL AND lease_expires_at IS NULL) OR "
            "(phase <> 'idle' AND run_id IS NOT NULL AND owner IS NOT NULL "
            "AND lease_expires_at IS NOT NULL)",
            name="lease_shape",
        ),
    )
    id: Mapped[int] = MappedColumn[int](Integer, primary_key=True)
    run_id: Mapped[UUID | None] = MappedColumn[UUID | None](
        ForeignKey("docker_run_state.run_id", ondelete="RESTRICT"), unique=True
    )
    phase: Mapped[str] = MappedColumn[str](String(16), server_default=text("'idle'"))
    owner: Mapped[str | None] = MappedColumn[str | None](String(128))
    fence: Mapped[int] = MappedColumn[int](BigInteger, server_default=text("0"))
    revision: Mapped[int] = MappedColumn[int](BigInteger, server_default=text("0"))
    cleanup_attempts: Mapped[int] = MappedColumn[int](Integer, server_default=text("0"))
    lease_expires_at: Mapped[datetime | None] = MappedColumn[datetime | None](
        DateTime(timezone=True)
    )


class DockerResource(Base):
    __tablename__ = "docker_resources"
    __table_args__ = (
        CheckConstraint("ordinal BETWEEN 1 AND 64 AND slot BETWEEN 0 AND 3", name="bounds"),
        CheckConstraint(
            "(kind = 'static' AND state IN ('observed','draining','released')) OR "
            "(kind = 'dynamic' AND state IN ('prepared','creating','observed','starting',"
            "'started','draining','removed','abandoned','conflict','unresolved'))",
            name="kind_state",
        ),
        CheckConstraint(
            "container_id IS NULL OR container_id ~ '^[0-9a-f]{64}$'", name="container_id"
        ),
        UniqueConstraint("run_id", "ordinal"),
        Index(
            "uq_docker_resource_active_slot",
            "run_id",
            "slot",
            unique=True,
            postgresql_where=text("state NOT IN ('removed','abandoned','released')"),
        ),
        Index(
            "uq_docker_resource_active_container",
            "container_id",
            unique=True,
            postgresql_where=text(
                "container_id IS NOT NULL AND state NOT IN ('removed','abandoned','released')"
            ),
        ),
        Index(
            "uq_docker_resource_dynamic_name",
            "name",
            unique=True,
            postgresql_where=text("kind = 'dynamic'"),
        ),
    )
    id: Mapped[UUID] = MappedColumn[UUID](primary_key=True)
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey("docker_run_state.run_id", ondelete="RESTRICT")
    )
    ordinal: Mapped[int] = MappedColumn[int](Integer)
    slot: Mapped[int] = MappedColumn[int](Integer)
    kind: Mapped[str] = MappedColumn[str](String(16))
    state: Mapped[str] = MappedColumn[str](String(16))
    name: Mapped[str] = MappedColumn[str](String(128))
    container_id: Mapped[str | None] = MappedColumn[str | None](String(64))
    definition: Mapped[dict[str, object]] = MappedColumn[dict[str, object]](JSONB)
    definition_digest: Mapped[str] = MappedColumn[str](String(64))
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )
    updated_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True),
        server_default=func.clock_timestamp(),
        onupdate=func.clock_timestamp(),
    )


class DockerPublication(Base):
    __tablename__ = "docker_publications"
    run_id: Mapped[UUID] = MappedColumn[UUID](
        ForeignKey("docker_run_state.run_id", ondelete="RESTRICT"), primary_key=True
    )
    manifest: Mapped[dict[str, object]] = MappedColumn[dict[str, object]](JSONB)
    digest: Mapped[str] = MappedColumn[str](String(64))
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )

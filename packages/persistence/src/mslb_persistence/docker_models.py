"""Relational definitions owned by docker."""

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

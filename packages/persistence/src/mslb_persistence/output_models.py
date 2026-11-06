"""Relational definitions owned by output."""

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
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, MappedColumn

from mslb_persistence.orm_base import Base


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

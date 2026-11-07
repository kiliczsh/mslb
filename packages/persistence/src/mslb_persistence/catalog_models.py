"""Relational definitions owned by catalog."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Integer,
    String,
    func,
    text,
)
from sqlalchemy.orm import Mapped, MappedColumn

from mslb_persistence.orm_base import Base


class Installation(Base):
    """Record one-time seeding and serialize coherent catalog revision changes."""

    __tablename__ = "installation"
    __table_args__ = (CheckConstraint(sqltext="id = 1", name="singleton"),)
    id: Mapped[int] = MappedColumn[int](Integer, primary_key=True)
    seeded: Mapped[bool] = MappedColumn[bool](Boolean, server_default=text(text="false"))
    revision: Mapped[int] = MappedColumn[int](BigInteger, server_default=text(text="0"))


class ManagedServer(Base):
    """Store the editable current version of a logical server."""

    __tablename__ = "managed_servers"
    __table_args__ = (
        CheckConstraint(sqltext="version > 0", name="version_positive"),
        CheckConstraint(
            sqltext="cpu_units_per_tick BETWEEN 1 AND 1000000000 "
            "AND mem_mb BETWEEN 1 AND 1000000000 "
            "AND rate_limit_per_sec BETWEEN 1 AND 1000000000",
            name="positive_capacities",
        ),
    )
    id: Mapped[str] = MappedColumn[str](String(length=128), primary_key=True)
    version: Mapped[int] = MappedColumn[int](Integer)
    cpu_units_per_tick: Mapped[int] = MappedColumn[int](Integer)
    mem_mb: Mapped[int] = MappedColumn[int](Integer)
    rate_limit_per_sec: Mapped[int] = MappedColumn[int](Integer)


class ServerHistory(Base):
    """Retain immutable capacity versions after later edits or deletion."""

    __tablename__ = "server_history"
    server_id: Mapped[str] = MappedColumn[str](String(length=128), primary_key=True)
    version: Mapped[int] = MappedColumn[int](Integer, primary_key=True)
    deleted: Mapped[bool] = MappedColumn[bool](Boolean)
    cpu_units_per_tick: Mapped[int] = MappedColumn[int](Integer)
    mem_mb: Mapped[int] = MappedColumn[int](Integer)
    rate_limit_per_sec: Mapped[int] = MappedColumn[int](Integer)
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )


class Dataset(Base):
    """Reference immutable normalized input and optional original-upload provenance."""

    __tablename__ = "datasets"
    __table_args__ = (
        CheckConstraint(
            sqltext="capacity_profile IN ('standard','stress') "
            "AND request_count BETWEEN 1 AND CASE WHEN capacity_profile = 'stress' "
            "THEN 50000 ELSE 10000 END AND size_bytes BETWEEN 1 AND 10485760",
            name="bounded_population",
        ),
        CheckConstraint(
            sqltext="earliest_arrival >= 0 AND latest_arrival >= earliest_arrival "
            "AND latest_arrival < 1000000",
            name="arrival_range",
        ),
        CheckConstraint(
            sqltext="(original_digest IS NULL AND source_size_bytes IS NULL) OR "
            "(original_digest IS NOT NULL AND source_size_bytes IS NOT NULL "
            "AND source_size_bytes BETWEEN 1 AND 10485760)",
            name="source_provenance",
        ),
    )
    id: Mapped[UUID] = MappedColumn[UUID](primary_key=True)
    digest: Mapped[str] = MappedColumn[str](String(length=64), unique=True)
    storage_key: Mapped[str] = MappedColumn[str](String(length=256))
    source_filename: Mapped[str] = MappedColumn[str](String(length=255))
    request_count: Mapped[int] = MappedColumn[int](Integer)
    capacity_profile: Mapped[str] = MappedColumn[str](
        String(length=16), server_default=text(text="'standard'"), default="standard"
    )
    earliest_arrival: Mapped[int] = MappedColumn[int](Integer)
    latest_arrival: Mapped[int] = MappedColumn[int](Integer)
    size_bytes: Mapped[int] = MappedColumn[int](Integer)
    original_digest: Mapped[str | None] = MappedColumn[str | None](String(length=64))
    source_size_bytes: Mapped[int | None] = MappedColumn[int | None](Integer)
    created_at: Mapped[datetime] = MappedColumn[datetime](
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )

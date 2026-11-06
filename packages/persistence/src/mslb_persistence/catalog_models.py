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

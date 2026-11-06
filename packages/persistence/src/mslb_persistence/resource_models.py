"""Relational definitions owned by resource."""

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Integer,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, MappedColumn

from mslb_persistence.orm_base import Base


class ResourceEvidence(Base):
    """Retain one fenced publisher report and its verified content digest."""

    __tablename__ = "resource_evidence"
    __table_args__ = (
        CheckConstraint(
            sqltext="id = 1 AND fence >= 0 AND revision >= 0", name="singleton_versions"
        ),
        CheckConstraint(
            sqltext="(owner IS NULL AND lease_expires_at IS NULL) OR "
            "(owner IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="lease_shape",
        ),
        CheckConstraint(
            sqltext="(decision IS NULL AND probe IS NULL AND digest IS NULL "
            "AND received_at IS NULL) OR "
            "(decision IS NOT NULL AND digest IS NOT NULL "
            "AND digest ~ '^[0-9a-f]{64}$' AND received_at IS NOT NULL)",
            name="report_shape",
        ),
    )
    id: Mapped[int] = MappedColumn[int](type_=Integer, primary_key=True)
    owner: Mapped[str | None] = MappedColumn[str | None](type_=String(length=128))
    fence: Mapped[int] = MappedColumn[int](type_=BigInteger, server_default=text("0"))
    revision: Mapped[int] = MappedColumn[int](type_=BigInteger, server_default=text("0"))
    lease_expires_at: Mapped[datetime | None] = MappedColumn[datetime | None](
        type_=DateTime(timezone=True)
    )
    received_at: Mapped[datetime | None] = MappedColumn[datetime | None](
        type_=DateTime(timezone=True)
    )
    probe: Mapped[dict[str, object] | None] = MappedColumn[dict[str, object] | None](
        type_=JSONB(none_as_null=True)
    )
    decision: Mapped[dict[str, object] | None] = MappedColumn[dict[str, object] | None](
        type_=JSONB(none_as_null=True)
    )
    digest: Mapped[str | None] = MappedColumn[str | None](type_=String(length=64))

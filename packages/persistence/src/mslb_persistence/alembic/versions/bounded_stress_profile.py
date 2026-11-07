"""Preserve normal dataset capacity while admitting an explicit bounded stress profile."""

import sqlalchemy as sa
from alembic import op

revision = "bounded_stress_profile"
down_revision = "resource_evidence"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    """Add an installation-derived profile and a conditional population constraint."""
    op.add_column(
        table_name="datasets",
        column=sa.Column(
            name="capacity_profile",
            type_=sa.String(length=16),
            nullable=False,
            server_default="standard",
        ),
    )
    op.drop_constraint(
        constraint_name=op.f(name="ck_datasets_bounded_population"),
        table_name="datasets",
        type_="check",
    )
    op.create_check_constraint(
        constraint_name=op.f(name="ck_datasets_bounded_population"),
        table_name="datasets",
        condition="capacity_profile IN ('standard','stress') "
        "AND request_count BETWEEN 1 AND CASE WHEN capacity_profile = 'stress' "
        "THEN 50000 ELSE 10000 END AND size_bytes BETWEEN 1 AND 10485760",
    )


def downgrade() -> None:
    """Refuse incompatible data rather than delete or silently reinterpret stress records."""
    connection = op.get_bind()
    present = connection.scalar(
        statement=sa.text(
            text="SELECT EXISTS(SELECT 1 FROM datasets WHERE capacity_profile = 'stress')"
        )
    )
    if present:
        raise RuntimeError("Cannot downgrade while stress datasets remain")
    op.drop_constraint(
        constraint_name=op.f(name="ck_datasets_bounded_population"),
        table_name="datasets",
        type_="check",
    )
    op.create_check_constraint(
        constraint_name=op.f(name="ck_datasets_bounded_population"),
        table_name="datasets",
        condition="request_count BETWEEN 1 AND 10000 AND size_bytes BETWEEN 1 AND 10485760",
    )
    op.drop_column(table_name="datasets", column_name="capacity_profile")

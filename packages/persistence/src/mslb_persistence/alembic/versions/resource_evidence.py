"""Fence latest resource evidence."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "resource_evidence"
down_revision = "docker_control"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    op.create_table(
        "resource_evidence",
        sa.Column(name="id", type_=sa.Integer(), nullable=False),
        sa.Column(name="owner", type_=sa.String(length=128), nullable=True),
        sa.Column(
            name="fence", type_=sa.BigInteger(), server_default=sa.text(text="0"), nullable=False
        ),
        sa.Column(
            name="revision", type_=sa.BigInteger(), server_default=sa.text(text="0"), nullable=False
        ),
        sa.Column(name="lease_expires_at", type_=sa.DateTime(timezone=True), nullable=True),
        sa.Column(name="received_at", type_=sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            name="probe",
            type_=postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            name="decision",
            type_=postgresql.JSONB(none_as_null=True, astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(name="digest", type_=sa.String(length=64), nullable=True),
        sa.CheckConstraint(
            sqltext="(decision IS NULL AND probe IS NULL AND digest IS NULL "
            "AND received_at IS NULL) OR (decision IS NOT NULL AND digest IS NOT NULL "
            "AND digest ~ '^[0-9a-f]{64}$' AND received_at IS NOT NULL)",
            name=op.f(name="ck_resource_evidence_report_shape"),
        ),
        sa.CheckConstraint(
            sqltext="(owner IS NULL AND lease_expires_at IS NULL) OR "
            "(owner IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name=op.f(name="ck_resource_evidence_lease_shape"),
        ),
        sa.CheckConstraint(
            sqltext="id = 1 AND fence >= 0 AND revision >= 0",
            name=op.f(name="ck_resource_evidence_singleton_versions"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f(name="pk_resource_evidence")),
    )
    op.execute(sqltext="INSERT INTO resource_evidence (id) VALUES (1)")


def downgrade() -> None:
    op.drop_table(table_name="resource_evidence")

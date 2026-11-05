"""Retain source-upload provenance without inventing bytes for legacy metadata."""

import sqlalchemy as sa
from alembic import op

revision = "dataset_sources"
down_revision = "durable_jobs"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    op.add_column("datasets", sa.Column("original_digest", sa.String(64), nullable=True))
    op.add_column("datasets", sa.Column("source_size_bytes", sa.Integer(), nullable=True))
    op.create_check_constraint(
        "ck_datasets_source_provenance",
        "datasets",
        "(original_digest IS NULL AND source_size_bytes IS NULL) OR "
        "(original_digest IS NOT NULL AND source_size_bytes IS NOT NULL "
        "AND source_size_bytes BETWEEN 1 AND 10485760)",
    )


def downgrade() -> None:
    op.drop_constraint("ck_datasets_source_provenance", "datasets", type_="check")
    op.drop_column("datasets", "source_size_bytes")
    op.drop_column("datasets", "original_digest")

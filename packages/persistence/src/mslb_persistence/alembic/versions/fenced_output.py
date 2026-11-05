"""Commit fenced chunks, indexed events, checkpoints, seals, and artifact pointers."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "fenced_output"
down_revision = "dataset_sources"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    op.create_table(
        "committed_chunks",
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=40), nullable=False),
        sa.Column("first_sequence", sa.Integer(), nullable=False),
        sa.Column("last_sequence", sa.Integer(), nullable=False),
        sa.Column("byte_offset", sa.Integer(), nullable=False),
        sa.Column("first_tick", sa.Integer(), nullable=False),
        sa.Column("last_tick", sa.Integer(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("fence", sa.BigInteger(), nullable=False),
        sa.Column("file", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.CheckConstraint(
            "first_sequence > 0 AND last_sequence >= first_sequence AND byte_offset >= 0",
            name=op.f("ck_committed_chunks_contiguous_range"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["jobs.run_id"],
            name=op.f("fk_committed_chunks_run_id_jobs"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "run_id", "name", "first_sequence", name=op.f("pk_committed_chunks")
        ),
    )
    op.create_table(
        "indexed_events",
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("t", sa.Integer(), nullable=False),
        sa.Column("event", sa.String(length=32), nullable=False),
        sa.Column("request_id", sa.String(length=128), nullable=False),
        sa.Column("server_id", sa.String(length=128), nullable=True),
        sa.Column("reason", sa.String(length=32), nullable=True),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["jobs.run_id"],
            name=op.f("fk_indexed_events_run_id_jobs"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("run_id", "sequence", name=op.f("pk_indexed_events")),
        sa.UniqueConstraint("run_id", "event", "request_id", name="uq_indexed_event_lifecycle"),
    )
    op.create_index(
        "ix_indexed_events_tick", "indexed_events", ["run_id", "t", "sequence"], unique=False
    )
    op.create_table(
        "output_progress",
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("reference", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("next_tick", sa.Integer(), nullable=False),
        sa.Column("executed_intervals", sa.Integer(), nullable=False),
        sa.Column("prefixes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("metrics", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.CheckConstraint(
            "kind IN ('checkpoint','terminal') AND revision > 0",
            name=op.f("ck_output_progress_kind_revision"),
        ),
        sa.CheckConstraint(
            "next_tick BETWEEN 0 AND 1000000 AND executed_intervals BETWEEN 0 AND 1000000",
            name=op.f("ck_output_progress_bounded_progress"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["jobs.run_id"],
            name=op.f("fk_output_progress_run_id_jobs"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("run_id", name=op.f("pk_output_progress")),
    )
    op.create_table(
        "published_artifacts",
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("manifest", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("bundle_digest", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["jobs.run_id"],
            name=op.f("fk_published_artifacts_run_id_jobs"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("run_id", name=op.f("pk_published_artifacts")),
    )
    op.create_table(
        "saved_checkpoints",
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("fence", sa.BigInteger(), nullable=False),
        sa.Column("reference", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("next_tick", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["jobs.run_id"],
            name=op.f("fk_saved_checkpoints_run_id_jobs"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("run_id", "revision", name=op.f("pk_saved_checkpoints")),
    )


def downgrade() -> None:
    op.drop_table("saved_checkpoints")
    op.drop_table("published_artifacts")
    op.drop_table("output_progress")
    op.drop_index("ix_indexed_events_tick", table_name="indexed_events")
    op.drop_table("indexed_events")
    op.drop_table("committed_chunks")

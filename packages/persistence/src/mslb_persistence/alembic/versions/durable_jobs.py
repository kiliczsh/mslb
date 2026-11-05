"""Create versioned catalog, immutable runs, outbox, and leased logical jobs."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "durable_jobs"
down_revision: str | None = None
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    op.create_table(
        "datasets",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("digest", sa.String(length=64), nullable=False),
        sa.Column("storage_key", sa.String(length=256), nullable=False),
        sa.Column("source_filename", sa.String(length=255), nullable=False),
        sa.Column("request_count", sa.Integer(), nullable=False),
        sa.Column("earliest_arrival", sa.Integer(), nullable=False),
        sa.Column("latest_arrival", sa.Integer(), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "earliest_arrival >= 0 AND latest_arrival >= earliest_arrival "
            "AND latest_arrival < 1000000",
            name=op.f("ck_datasets_arrival_range"),
        ),
        sa.CheckConstraint(
            "request_count BETWEEN 1 AND 10000 AND size_bytes BETWEEN 1 AND 10485760",
            name=op.f("ck_datasets_bounded_population"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_datasets")),
        sa.UniqueConstraint("digest", name=op.f("uq_datasets_digest")),
    )
    op.create_table(
        "installation",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("seeded", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("revision", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.CheckConstraint("id = 1", name=op.f("ck_installation_singleton")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_installation")),
    )
    op.create_table(
        "managed_servers",
        sa.Column("id", sa.String(length=128), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("cpu_units_per_tick", sa.Integer(), nullable=False),
        sa.Column("mem_mb", sa.Integer(), nullable=False),
        sa.Column("rate_limit_per_sec", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "cpu_units_per_tick BETWEEN 1 AND 1000000000 "
            "AND mem_mb BETWEEN 1 AND 1000000000 "
            "AND rate_limit_per_sec BETWEEN 1 AND 1000000000",
            name=op.f("ck_managed_servers_positive_capacities"),
        ),
        sa.CheckConstraint("version > 0", name=op.f("ck_managed_servers_version_positive")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_managed_servers")),
    )
    op.create_table(
        "server_history",
        sa.Column("server_id", sa.String(length=128), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("deleted", sa.Boolean(), nullable=False),
        sa.Column("cpu_units_per_tick", sa.Integer(), nullable=False),
        sa.Column("mem_mb", sa.Integer(), nullable=False),
        sa.Column("rate_limit_per_sec", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("server_id", "version", name=op.f("pk_server_history")),
    )
    op.create_table(
        "runs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("dataset_id", sa.Uuid(), nullable=False),
        sa.Column("snapshot_digest", sa.String(length=64), nullable=False),
        sa.Column("snapshot_key", sa.String(length=256), nullable=False),
        sa.Column("dataset_digest", sa.String(length=64), nullable=False),
        sa.Column("server_snapshot_digest", sa.String(length=64), nullable=False),
        sa.Column("engine_version", sa.String(length=64), nullable=False),
        sa.Column("configuration", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("server_versions", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["dataset_id"],
            ["datasets.id"],
            name=op.f("fk_runs_dataset_id_datasets"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_runs")),
    )
    op.create_index(op.f("ix_runs_created_at"), "runs", ["created_at"], unique=False)
    op.create_table(
        "idempotency_records",
        sa.Column("scope", sa.String(length=128), nullable=False),
        sa.Column("key_hash", sa.String(length=64), nullable=False),
        sa.Column("body_digest", sa.String(length=64), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["runs.id"],
            name=op.f("fk_idempotency_records_run_id_runs"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("scope", "key_hash", name=op.f("pk_idempotency_records")),
    )
    op.create_index(
        op.f("ix_idempotency_records_expires_at"),
        "idempotency_records",
        ["expires_at"],
        unique=False,
    )
    op.create_table(
        "jobs",
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column(
            "status", sa.String(length=32), server_default=sa.text("'queued'"), nullable=False
        ),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("fence", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("owner", sa.String(length=128), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "(status IN ('running','cancel_requested') AND owner IS NOT NULL "
            "AND lease_expires_at IS NOT NULL) OR "
            "(status NOT IN ('running','cancel_requested') AND owner IS NULL "
            "AND lease_expires_at IS NULL)",
            name=op.f("ck_jobs_lease_shape"),
        ),
        sa.CheckConstraint(
            "status IN ('queued','running','cancel_requested','completed','failed','cancelled')",
            name=op.f("ck_jobs_status"),
        ),
        sa.CheckConstraint(
            "attempts BETWEEN 0 AND 3 AND fence >= 0", name=op.f("ck_jobs_attempt_fence")
        ),
        sa.ForeignKeyConstraint(
            ["run_id"], ["runs.id"], name=op.f("fk_jobs_run_id_runs"), ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("run_id", name=op.f("pk_jobs")),
    )
    op.create_index("ix_jobs_claim", "jobs", ["status", "created_at", "run_id"], unique=False)
    op.create_table(
        "attempts",
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("number", sa.Integer(), nullable=False),
        sa.Column("fence", sa.BigInteger(), nullable=False),
        sa.Column("owner", sa.String(length=128), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(
            ["run_id"], ["jobs.run_id"], name=op.f("fk_attempts_run_id_jobs"), ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("run_id", "number", name=op.f("pk_attempts")),
        sa.UniqueConstraint("run_id", "fence", name=op.f("uq_attempts_run_id")),
    )
    op.create_table(
        "execution_slot",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=True),
        sa.CheckConstraint("id = 1", name=op.f("ck_execution_slot_singleton")),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["jobs.run_id"],
            name=op.f("fk_execution_slot_run_id_jobs"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_execution_slot")),
        sa.UniqueConstraint("run_id", name=op.f("uq_execution_slot_run_id")),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["run_id"], ["jobs.run_id"], name=op.f("fk_outbox_run_id_jobs"), ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox")),
        sa.UniqueConstraint("run_id", name=op.f("uq_outbox_run_id")),
    )
    op.create_index(op.f("ix_outbox_delivered_at"), "outbox", ["delivered_at"], unique=False)
    op.execute(sa.text("INSERT INTO installation (id) VALUES (1)"))
    op.execute(sa.text("INSERT INTO execution_slot (id) VALUES (1)"))


def downgrade() -> None:
    op.drop_index(op.f("ix_outbox_delivered_at"), table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("execution_slot")
    op.drop_table("attempts")
    op.drop_index("ix_jobs_claim", table_name="jobs")
    op.drop_table("jobs")
    op.drop_index(op.f("ix_idempotency_records_expires_at"), table_name="idempotency_records")
    op.drop_table("idempotency_records")
    op.drop_index(op.f("ix_runs_created_at"), table_name="runs")
    op.drop_table("runs")
    op.drop_table("server_history")
    op.drop_table("managed_servers")
    op.drop_table("installation")
    op.drop_table("datasets")

"""Fence Docker control, retain unresolved resources, and separate job modes."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "docker_control"
down_revision = "fenced_output"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:

    op.create_table(
        "docker_deployments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("definition", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_docker_deployments")),
        sa.UniqueConstraint("fingerprint", name=op.f("uq_docker_deployments_fingerprint")),
    )
    op.create_table(
        "docker_run_state",
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("deployment_id", sa.Uuid(), nullable=False),
        sa.Column("snapshot", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("desired_status", sa.String(length=16), nullable=True),
        sa.Column("desired_error", sa.String(length=64), nullable=True),
        sa.CheckConstraint(
            "desired_status IS NULL OR desired_status IN ('completed','failed','cancelled')",
            name=op.f("ck_docker_run_state_desired_status"),
        ),
        sa.ForeignKeyConstraint(
            ["deployment_id"],
            ["docker_deployments.id"],
            name=op.f("fk_docker_run_state_deployment_id_docker_deployments"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["runs.id"],
            name=op.f("fk_docker_run_state_run_id_runs"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("run_id", name=op.f("pk_docker_run_state")),
    )
    op.create_table(
        "docker_control_slot",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=True),
        sa.Column("phase", sa.String(length=16), server_default=sa.text("'idle'"), nullable=False),
        sa.Column("owner", sa.String(length=128), nullable=True),
        sa.Column("fence", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("revision", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("cleanup_attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "(phase = 'idle' AND run_id IS NULL AND owner IS NULL AND lease_expires_at IS NULL) OR "
            "(phase <> 'idle' AND run_id IS NOT NULL AND owner IS NOT NULL "
            "AND lease_expires_at IS NOT NULL)",
            name=op.f("ck_docker_control_slot_lease_shape"),
        ),
        sa.CheckConstraint(
            "phase IN ('idle','running','cleanup')", name=op.f("ck_docker_control_slot_phase")
        ),
        sa.CheckConstraint(
            "id = 1 AND fence >= 0 AND revision >= 0 AND cleanup_attempts BETWEEN 0 AND 3",
            name=op.f("ck_docker_control_slot_singleton_fence"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["docker_run_state.run_id"],
            name=op.f("fk_docker_control_slot_run_id_docker_run_state"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_docker_control_slot")),
        sa.UniqueConstraint("run_id", name=op.f("uq_docker_control_slot_run_id")),
    )
    op.create_table(
        "docker_publications",
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("manifest", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("digest", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["docker_run_state.run_id"],
            name=op.f("fk_docker_publications_run_id_docker_run_state"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("run_id", name=op.f("pk_docker_publications")),
    )
    op.create_table(
        "docker_resources",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("run_id", sa.Uuid(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("slot", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("container_id", sa.String(length=64), nullable=True),
        sa.Column("definition", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("definition_digest", sa.String(length=64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("clock_timestamp()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(kind = 'static' AND state IN ('observed','draining','released')) OR "
            "(kind = 'dynamic' AND state IN ('prepared','creating','observed','starting',"
            "'started','draining','removed','abandoned','conflict','unresolved'))",
            name=op.f("ck_docker_resources_kind_state"),
        ),
        sa.CheckConstraint(
            "container_id IS NULL OR container_id ~ '^[0-9a-f]{64}$'",
            name=op.f("ck_docker_resources_container_id"),
        ),
        sa.CheckConstraint(
            "ordinal BETWEEN 1 AND 64 AND slot BETWEEN 0 AND 3",
            name=op.f("ck_docker_resources_bounds"),
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["docker_run_state.run_id"],
            name=op.f("fk_docker_resources_run_id_docker_run_state"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_docker_resources")),
        sa.UniqueConstraint("run_id", "ordinal", name=op.f("uq_docker_resources_run_id")),
    )
    op.create_index(
        "uq_docker_resource_active_container",
        "docker_resources",
        ["container_id"],
        unique=True,
        postgresql_where=sa.text(
            "container_id IS NOT NULL AND state NOT IN ('removed','abandoned','released')"
        ),
    )
    op.create_index(
        "uq_docker_resource_active_slot",
        "docker_resources",
        ["run_id", "slot"],
        unique=True,
        postgresql_where=sa.text("state NOT IN ('removed','abandoned','released')"),
    )
    op.create_index(
        "uq_docker_resource_dynamic_name",
        "docker_resources",
        ["name"],
        unique=True,
        postgresql_where=sa.text("kind = 'dynamic'"),
    )
    op.add_column(
        "jobs",
        sa.Column(
            "kind", sa.String(length=16), server_default=sa.text("'logical'"), nullable=False
        ),
    )
    op.create_index(
        "ix_jobs_kind_claim", "jobs", ["kind", "status", "created_at", "run_id"], unique=False
    )

    op.create_check_constraint(op.f("ck_jobs_kind"), "jobs", "kind IN ('logical','docker')")
    op.execute("INSERT INTO docker_control_slot (id) VALUES (1)")


def downgrade() -> None:

    op.drop_index("ix_jobs_kind_claim", table_name="jobs")
    op.drop_constraint(op.f("ck_jobs_kind"), "jobs", type_="check")
    op.drop_column("jobs", "kind")
    op.drop_index(
        "uq_docker_resource_dynamic_name",
        table_name="docker_resources",
        postgresql_where=sa.text("kind = 'dynamic'"),
    )
    op.drop_index(
        "uq_docker_resource_active_slot",
        table_name="docker_resources",
        postgresql_where=sa.text("state NOT IN ('removed','abandoned','released')"),
    )
    op.drop_index(
        "uq_docker_resource_active_container",
        table_name="docker_resources",
        postgresql_where=sa.text(
            "container_id IS NOT NULL AND state NOT IN ('removed','abandoned','released')"
        ),
    )
    op.drop_table("docker_resources")
    op.drop_table("docker_publications")
    op.drop_table("docker_control_slot")
    op.drop_table("docker_run_state")
    op.drop_table("docker_deployments")

"""Keep one bounded fenced live Docker observation frame per run."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "docker_progress"
down_revision = "bounded_stress_profile"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    """Add only an optional bounded observation cache; immutable results stay separate."""
    op.add_column(
        table_name="docker_run_state",
        column=sa.Column(
            name="latest_progress", type_=postgresql.JSONB(none_as_null=True), nullable=True
        ),
    )
    op.create_check_constraint(
        constraint_name=op.f(name="ck_docker_run_state_latest_progress"),
        table_name="docker_run_state",
        condition="latest_progress IS NULL OR "
        "(jsonb_typeof(latest_progress) = 'object' "
        "AND octet_length(latest_progress::text) <= 16384)",
    )


def downgrade() -> None:
    """Discard the derived live cache without changing jobs or published results."""
    op.drop_constraint(
        constraint_name=op.f(name="ck_docker_run_state_latest_progress"),
        table_name="docker_run_state",
        type_="check",
    )
    op.drop_column(table_name="docker_run_state", column_name="latest_progress")

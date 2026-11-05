"""Alembic uses the existing async-engine connection through run_sync."""

from alembic import context
from mslb_persistence.models import Base
from sqlalchemy import Connection

connection = context.config.attributes.get("connection")
if not isinstance(connection, Connection):
    raise RuntimeError("Migrations require a managed PostgreSQL connection")
context.configure(connection=connection, target_metadata=Base.metadata, compare_type=True)
with context.begin_transaction():
    context.run_migrations()

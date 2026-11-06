"""Run packaged immutable Alembic revisions on a caller-owned connection."""

import asyncio
import os
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import Connection

from mslb_persistence.database import Database


def configuration() -> Config:
    config = Config()
    config.set_main_option(
        name="script_location", value=str(object=Path(__file__).parent / "alembic")
    )
    return config


async def migrate(database: Database, revision: str = "head") -> None:
    def upgrade(connection: Connection) -> None:
        config = configuration()
        config.attributes["connection"] = connection
        command.upgrade(config=config, revision=revision)

    async with database.engine.begin() as connection:
        await connection.run_sync(fn=upgrade)


async def main() -> None:
    database = Database(url=os.environ["DATABASE_URL"])
    try:
        await migrate(database=database)
        print("PostgreSQL migrations are at head")
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main=main())

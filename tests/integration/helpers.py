"""Disposable project schemas and unchanged fixture readers for integration tests."""

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

from mslb_contracts.inputs import RequestInput, ServerInput, read_requests, read_servers
from mslb_persistence.database import Database
from mslb_persistence.migrations import migrate
from sqlalchemy.schema import CreateSchema, DropSchema


@asynccontextmanager
async def scratch(*, retain_on_failure: bool = False) -> AsyncGenerator[Database]:
    url = os.environ["TEST_DATABASE_URL"]
    schema = "mslb_test_" + uuid4().hex
    admin, database = Database(url), Database(url, schema)
    async with admin.engine.begin() as connection:
        await connection.execute(CreateSchema(schema))
    try:
        await migrate(database)
        yield database
    except BaseException:
        await database.close()
        if not retain_on_failure:
            async with admin.engine.begin() as connection:
                await connection.execute(DropSchema(schema, cascade=True))
        else:
            print("Retained lifecycle failure ownership schema:", schema)
        raise
    else:
        await database.close()
        async with admin.engine.begin() as connection:
            await connection.execute(DropSchema(schema, cascade=True))
    finally:
        await admin.close()


def fixture_inputs() -> tuple[tuple[ServerInput, ...], tuple[RequestInput, ...], int]:
    with Path("/fixtures/core/servers.json").open("rb") as stream:
        servers = read_servers(stream)
    with Path("/fixtures/core/requests.csv").open("rb") as stream:
        requests = read_requests(stream)
    return servers, requests, Path("/fixtures/core/requests.csv").stat().st_size

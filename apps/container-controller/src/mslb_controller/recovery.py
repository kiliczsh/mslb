"""Trusted local operator retry for expired stalled physical cleanup."""

import argparse
import asyncio
import os
import sys
from uuid import UUID

from mslb_persistence.database import Database
from mslb_persistence.docker_control import DockerControl
from mslb_persistence.errors import StoreError


async def retry(*, run_id: UUID) -> None:
    """Grant another bounded reconciliation window without releasing any resource."""
    database = Database(
        url=os.environ["DATABASE_URL"],
        schema=os.environ.get(key="CONTROL_DATABASE_SCHEMA", default="public"),
        enforce_resource_guard=True,
    )
    try:
        await DockerControl(database=database).retry_cleanup(run_id=run_id)
    finally:
        await database.close()
    print(f"Cleanup retry requested for {run_id}; resources remain reserved until reconciled.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    arguments = parser.parse_args()
    try:
        asyncio.run(main=retry(run_id=UUID(hex=arguments.run_id)))
    except StoreError as error:
        print(f"Cleanup retry refused ({error.code}).", file=sys.stderr)
        sys.exit(2)

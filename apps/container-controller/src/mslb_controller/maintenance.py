"""Docker-only bounded project output maintenance, dry-run unless explicitly applied."""

import argparse
import asyncio
import json
import os
from dataclasses import asdict
from pathlib import Path

from mslb_application.file_maintenance import FileMaintenance
from mslb_application.managed_files import ManagedFiles
from mslb_persistence.database import Database


async def main(*, apply: bool, limit: int, min_age_seconds: int) -> None:
    """Plan or apply managed-output maintenance using the configured database."""
    database = Database(
        url=os.environ["DATABASE_URL"],
        schema=os.environ.get(key="DATABASE_SCHEMA", default="public"),
    )
    try:
        files = ManagedFiles(root=Path("/data/outputs"))
        report = await FileMaintenance(database=database, files=files).run(
            apply=apply, limit=limit, min_age_seconds=min_age_seconds
        )
        print(json.dumps(obj=asdict(obj=report), sort_keys=True))
    finally:
        await database.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--min-age-seconds", type=int, default=3600)
    arguments = parser.parse_args()
    asyncio.run(
        main=main(
            apply=arguments.apply, limit=arguments.limit, min_age_seconds=arguments.min_age_seconds
        )
    )

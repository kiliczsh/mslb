"""Submit a durable local run and verify completion by the actual Compose worker."""

import asyncio
import json
import os
import time
from pathlib import Path
from typing import BinaryIO

from mslb_application.artifacts import verify_bundle
from mslb_application.files import FileReference, ManagedFiles
from mslb_application.submissions import Submissions
from mslb_contracts.durability import StoredFile
from mslb_contracts.inputs import ServerInput, read_servers
from mslb_contracts.runs import BundleManifest, RunSettings
from mslb_persistence.catalog import Catalog
from mslb_persistence.database import Database
from mslb_persistence.durability import Durability
from mslb_persistence.jobs import Jobs


def seed_inputs() -> tuple[ServerInput, ...]:
    with Path("/fixtures/core/servers.json").open(mode="rb") as stream:
        return read_servers(stream=stream)


def verify_completion(outputs: ManagedFiles, pointer: StoredFile) -> BundleManifest:
    with outputs.verified_stream(
        reference=FileReference(
            key=pointer.key, sha256=pointer.sha256, size_bytes=pointer.size_bytes
        )
    ):
        return verify_bundle(directory=outputs.path(key=pointer.key).parent)


def open_requests() -> BinaryIO:
    return Path("/fixtures/core/requests.csv").open(mode="rb")


async def main() -> None:
    database = Database(url=os.environ["DATABASE_URL"])
    try:
        inputs = await asyncio.to_thread(ManagedFiles, root=Path("/data/inputs"))
        outputs = await asyncio.to_thread(
            ManagedFiles, root=Path("/data/outputs"), limits=None, read_only=True
        )
        catalog = Catalog(database=database)
        await catalog.seed_once(servers=await asyncio.to_thread(seed_inputs))
        service = Submissions(catalog=catalog, files=inputs)
        stream = await asyncio.to_thread(open_requests)
        try:
            dataset = await service.upload(source=stream, filename="requests.csv")
        finally:
            await asyncio.to_thread(stream.close)
        run = await service.submit(
            dataset_id=dataset.id, settings=RunSettings(strategy="round_robin")
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            state = await Jobs(database=database).get(run_id=run.id)
            if state.status == "completed":
                pointer = await Durability(database=database).published(run_id=run.id)

                manifest = await asyncio.to_thread(
                    verify_completion, outputs=outputs, pointer=pointer
                )
                print(
                    json.dumps(
                        obj={
                            "run_id": str(object=run.id),
                            "bundle_digest": manifest.bundle_digest,
                            "terminal_tick": manifest.terminal_tick,
                            "status": "worker completion verified",
                        }
                    )
                )
                return
            if state.status in {"failed", "cancelled"}:
                raise RuntimeError(
                    "Worker smoke run did not complete: " + str(object=state.error_code)
                )
            await asyncio.sleep(delay=0.2)
        raise RuntimeError("Compose worker did not finish the smoke run within its bound")
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main=main())

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
    with Path("/fixtures/core/servers.json").open("rb") as stream:
        return read_servers(stream)


def verify_completion(outputs: ManagedFiles, pointer: StoredFile) -> BundleManifest:
    with outputs.verified_stream(FileReference(pointer.key, pointer.sha256, pointer.size_bytes)):
        return verify_bundle(outputs.path(pointer.key).parent)


def open_requests() -> BinaryIO:
    return Path("/fixtures/core/requests.csv").open("rb")


async def main() -> None:
    database = Database(os.environ["DATABASE_URL"])
    try:
        inputs = await asyncio.to_thread(ManagedFiles, Path("/data/inputs"))
        outputs = await asyncio.to_thread(ManagedFiles, Path("/data/outputs"), None, True)
        catalog = Catalog(database)
        await catalog.seed_once(await asyncio.to_thread(seed_inputs))
        service = Submissions(catalog, inputs)
        stream = await asyncio.to_thread(open_requests)
        try:
            dataset = await service.upload(stream, "requests.csv")
        finally:
            await asyncio.to_thread(stream.close)
        run = await service.submit(dataset.id, RunSettings(strategy="round_robin"))
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            state = await Jobs(database).get(run.id)
            if state.status == "completed":
                pointer = await Durability(database).published(run.id)

                manifest = await asyncio.to_thread(verify_completion, outputs, pointer)
                print(
                    json.dumps(
                        dict(
                            run_id=str(run.id),
                            bundle_digest=manifest.bundle_digest,
                            terminal_tick=manifest.terminal_tick,
                            status="worker completion verified",
                        )
                    )
                )
                return
            if state.status in {"failed", "cancelled"}:
                raise RuntimeError("Worker smoke run did not complete: " + str(state.error_code))
            await asyncio.sleep(0.2)
        raise RuntimeError("Compose worker did not finish the smoke run within its bound")
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(main())

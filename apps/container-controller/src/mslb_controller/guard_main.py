"""Read-only guard role: publish decisions, with no Docker mutation credentials or ports."""

import asyncio
import os
import signal
from uuid import uuid4

from mslb_application.resource_guard import ResourcePublisher
from mslb_contracts.resources import ResourceProbe
from mslb_persistence.database import Database
from mslb_persistence.resource_evidence import EvidenceRecord, ResourceEvidenceStore

from mslb_controller.errors import EngineError
from mslb_controller.guard_health import write_health
from mslb_controller.host_memory import read_host_memory
from mslb_controller.resource_client import ResourceClient


async def main() -> None:
    """Run the fenced resource publisher with its independent health heartbeat."""
    database = Database(url=os.environ["DATABASE_URL"])
    client = ResourceClient()
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for code in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig=code, callback=stopped.set)

    async def measure() -> ResourceProbe | None:
        try:
            probe = await client.probe()
            if probe.desktop_engine:
                probe = probe.model_copy(
                    update={"host_memory": await asyncio.to_thread(read_host_memory)}
                )
            return probe
        except EngineError:
            return None

    async def published(_report: EvidenceRecord) -> None:
        # Liveness means current publication, not permission to admit a workload.
        await asyncio.to_thread(write_health)

    try:
        await ResourcePublisher(
            store=ResourceEvidenceStore(database=database),
            owner=uuid4().hex,
            measure=measure,
        ).run(stopped=stopped, published=published)
    finally:
        await client.close()
        await database.close()


if __name__ == "__main__":
    asyncio.run(main=main())

"""Real loopback HTTP, private PostgreSQL schema and owned files for MCP checks."""

import asyncio
import io
import socket
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import uvicorn
from mslb_api.app import create_app, get_mcp
from mslb_api.mcp_adapter import McpAdapter
from mslb_api.settings import Settings
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_application.worker import Worker
from mslb_contracts.http import SimulationCreate
from mslb_persistence.database import Database

from tests.integration.helpers import scratch


@dataclass(frozen=True, slots=True, kw_only=True)
class HttpWorkspace:
    database: Database
    adapter: McpAdapter
    inputs: ManagedFiles
    outputs: ManagedFiles
    url: str

    async def dataset(self, *, count: int = 3) -> UUID:
        source = (
            "t,request_id,work_units,mem_mb\n"
            + "".join(f"0,r{index:04d},1,1\n" for index in range(count))
        ).encode()
        dataset = await self.adapter.services.submissions.upload(
            source=io.BytesIO(initial_bytes=source), filename="requests.csv"
        )
        return dataset.id

    async def enqueue(self, *, count: int = 3, strategy: str = "first_fit") -> UUID:
        dataset_id = await self.dataset(count=count)
        accepted = await self.adapter.services.submit(
            body=SimulationCreate.model_validate(
                obj={"dataset_id": dataset_id, "strategy": strategy}
            )
        )
        return accepted.run_id


@asynccontextmanager
async def http_workspace(*, root: Path) -> AsyncGenerator[HttpWorkspace]:
    async with scratch() as database:
        limits = StorageLimits(min_free_bytes=0)
        inputs = await asyncio.to_thread(ManagedFiles, root=root / "inputs", limits=limits)
        outputs = await asyncio.to_thread(ManagedFiles, root=root / "outputs", limits=limits)
        listener = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        app = create_app(
            settings=Settings(allowed_hosts=(f"127.0.0.1:{port}",)),
            database=database,
            inputs=inputs,
            outputs=outputs,
            signing_key=b"isolated-http-mcp-test-signing-key" * 2,
        )
        server = uvicorn.Server(
            config=uvicorn.Config(app=app, log_level="warning", access_log=False, ws="none")
        )
        serving = asyncio.create_task(coro=server.serve(sockets=[listener]))
        try:
            await eventually(predicate=lambda: server.started)
            yield HttpWorkspace(
                database=database,
                adapter=get_mcp(app=app),
                inputs=inputs,
                outputs=outputs,
                url=f"http://127.0.0.1:{port}/mcp",
            )
        finally:
            server.should_exit = True
            await asyncio.wait_for(fut=serving, timeout=5)
            listener.close()


async def eventually(*, predicate: Callable[[], bool], seconds: float = 5) -> None:
    wakeup = asyncio.Event()
    async with asyncio.timeout(delay=seconds):
        while not predicate():
            with suppress(TimeoutError):
                await asyncio.wait_for(fut=wakeup.wait(), timeout=0.01)


def worker(*, workspace: HttpWorkspace) -> Worker:
    return Worker(database=workspace.database, inputs=workspace.inputs, outputs=workspace.outputs)

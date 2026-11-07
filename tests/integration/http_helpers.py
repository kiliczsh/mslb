"""Real API composition with private files and a disposable PostgreSQL schema."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from mslb_api.app import create_app
from mslb_api.settings import Settings
from mslb_application.files import ManagedFiles, StorageLimits

from tests.integration.helpers import scratch


@asynccontextmanager
async def workspace_client(
    *, directory: Path, authenticated: bool
) -> AsyncGenerator[httpx.AsyncClient]:
    """Seed unchanged servers and authenticate requests without mocking application services."""
    async with scratch() as database:
        inputs = await asyncio.to_thread(
            ManagedFiles, root=directory / "inputs", limits=StorageLimits(min_free_bytes=0)
        )
        outputs = await asyncio.to_thread(
            ManagedFiles, root=directory / "outputs", limits=StorageLimits(min_free_bytes=0)
        )
        token = "synthetic-workspace-contract-token-" * 2
        app = create_app(
            settings=Settings(
                auth_enabled=authenticated, token=token, allowed_hosts=("testserver",)
            ),
            database=database,
            inputs=inputs,
            outputs=outputs,
            signing_key=b"synthetic-workspace-cursor-key-" * 2,
        )
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver/api/v1/",
                headers={"Authorization": "Bearer " + token} if authenticated else {},
            ) as client,
        ):
            yield client

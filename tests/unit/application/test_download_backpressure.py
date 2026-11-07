"""A connected slow reader cannot retain verified files or API admission indefinitely."""

import asyncio
import os
from http import HTTPStatus
from pathlib import Path

import httpx
import pytest
from mslb_api import streams
from mslb_api.guard import Guard
from mslb_api.settings import Settings
from mslb_api.streams import ArtifactResponse
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_application.queries import Download
from starlette.responses import Response
from starlette.types import Message, Receive, Scope, Send


@pytest.mark.parametrize(argnames="archive", argvalues=[False, True])
@pytest.mark.parametrize(
    argnames="blocked_message", argvalues=["http.response.start", "http.response.body"]
)
@pytest.mark.parametrize(argnames="asgi_version", argvalues=["2.0", "2.4"])
def test_stalled_download_closes_files_and_releases_real_guard_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    archive: bool,
    blocked_message: str,
    asgi_version: str,
) -> None:
    monkeypatch.setattr(target=streams, name="SEND_TIMEOUT_SECONDS", value=0.05)
    files = ManagedFiles(root=tmp_path, limits=StorageLimits(min_free_bytes=0))
    reference = files.publish(
        key="datasets/stalled.csv", chunks=(os.urandom(2 * 1024**2),), maximum=3 * 1024**2
    )
    response = ArtifactResponse(
        downloads=(
            Download(
                files=files, reference=reference, filename="requests.csv", media_type="text/csv"
            ),
        ),
        archive=archive,
    )

    async def run() -> None:
        blocked = asyncio.Event()

        async def application(scope: Scope, receive: Receive, send: Send) -> None:
            if scope["path"].endswith("/artifact"):
                await response(scope, receive, send)
            else:
                await Response(content="ok")(scope, receive, send)

        async def receive() -> Message:
            await asyncio.Event().wait()
            return {"type": "http.disconnect"}

        async def send(message: Message) -> None:
            if message["type"] == blocked_message:
                blocked.set()
                await asyncio.Event().wait()

        guard = Guard(
            app=application, settings=Settings(allowed_hosts=("testserver",), request_limit=1)
        )
        scope: Scope = {
            "type": "http",
            "method": "GET",
            "path": "/api/v1/simulations/run/artifact",
            "headers": [(b"host", b"testserver")],
            "asgi": {"spec_version": asgi_version},
        }
        task = asyncio.create_task(coro=guard(scope, receive, send))
        try:
            await asyncio.wait_for(fut=blocked.wait(), timeout=2)
            assert guard.active == 1 and response.files
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=guard), base_url="http://testserver"
            ) as client:
                assert (
                    await client.get(url="/api/v1/servers")
                ).status_code == HTTPStatus.TOO_MANY_REQUESTS.value
                await asyncio.wait_for(fut=task, timeout=2)
                assert guard.active == 0
                assert all(stream.closed for stream in response.files)
                assert response.stopped.is_set()
                with pytest.raises(expected_exception=StopAsyncIteration):
                    await anext(response.source)
                assert (await client.get(url="/api/v1/servers")).status_code == HTTPStatus.OK.value
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(main=run())

import asyncio
import ipaddress
import json
import os
from collections.abc import AsyncGenerator
from pathlib import Path

import httpx
import pytest
from mslb_api.app import _Multipart
from mslb_api.guard import Guard
from mslb_api.settings import Settings, cursor_secret
from mslb_api.streams import ArtifactResponse
from mslb_application.admission import MaterializationAdmission
from mslb_application.errors import OperationError
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_application.queries import Download
from starlette.datastructures import Headers
from starlette.requests import ClientDisconnect
from starlette.responses import Response
from starlette.types import Message, Receive, Scope, Send

from scripts.check_deployment import check


def test_guard_auth_origin_host_preflight_and_bounded_chunked_body() -> None:
    async def scenario() -> None:
        async def application(scope: Scope, receive: Receive, send: Send) -> None:
            while True:
                message = await receive()
                if not message.get("more_body", False):
                    break
            await Response("ok")(scope, receive, send)

        token = "synthetic-test-token-" * 3
        policy = Settings(
            auth_enabled=True,
            token=token,
            allowed_hosts=("testserver",),
            allowed_origins=("https://dashboard.test",),
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=Guard(application, policy)),
            base_url="http://testserver",
        ) as client:
            assert (await client.get("/api/v1/servers")).status_code == 401
            assert (await client.get("/api/v1/health/live")).status_code == 200
            headers = {"Authorization": "Bearer " + token}
            for path in (
                "/api/v1/servers",
                "/api/v1/simulations/x/stream",
                "/api/v1/simulations/x/artifact",
                "/api/v1/openapi.json",
            ):
                response = await client.get(
                    path,
                    headers={"Authorization": "Bearer wrong", "Origin": "https://dashboard.test"},
                )
                assert response.status_code == 401 and token not in response.text
                assert response.headers["access-control-allow-origin"] == "https://dashboard.test"
            assert (
                await client.get("/", headers=headers | {"Host": "attacker.test"})
            ).status_code == 403
            assert (
                await client.get("/", headers=headers | {"Origin": "https://attacker.test"})
            ).status_code == 403
            valid = await client.get("/", headers=headers | {"Origin": "https://dashboard.test"})
            assert (
                valid.status_code == 200
                and valid.headers["access-control-allow-origin"] == "https://dashboard.test"
            )
            assert (
                valid.headers["x-request-id"]
                and valid.headers["x-content-type-options"] == "nosniff"
            )
            preflight = await client.options(
                "/api/v1/servers",
                headers={
                    "Origin": "https://dashboard.test",
                    "Access-Control-Request-Method": "POST",
                },
            )
            assert preflight.status_code == 204
            assert (await client.get("/?token=not-accepted", headers=headers)).status_code == 401
            assert (await client.get("/?%74oken=not-accepted", headers=headers)).status_code == 401

            async def body() -> AsyncGenerator[bytes]:
                for _ in range(20):
                    yield b"x" * 4096

            oversized = await client.post("/api/v1/servers", content=body(), headers=headers)
            assert oversized.status_code == 413
            assert oversized.json()["error"]["code"] == "PAYLOAD_TOO_LARGE"

    asyncio.run(scenario())


def test_request_upload_and_stream_admission_leave_health_available() -> None:
    async def scenario() -> None:
        entered, release = asyncio.Event(), asyncio.Event()

        async def application(scope: Scope, receive: Receive, send: Send) -> None:
            if scope["path"] != "/api/v1/health/live":
                entered.set()
                await release.wait()
            await Response("ok")(scope, receive, send)

        for path in ("/api/v1/datasets", "/api/v1/simulations/run/stream", "/api/v1/servers"):
            guard = Guard(
                application,
                Settings(
                    allowed_hosts=("testserver",),
                    request_limit=1 if path.endswith("servers") else 2,
                    stream_limit=1,
                ),
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=guard), base_url="http://testserver"
            ) as client:
                held = asyncio.create_task(client.post(path))
                await entered.wait()
                assert (await client.post(path)).status_code == 429
                assert (await client.get("/api/v1/health/live")).status_code == 200
                release.set()
                assert (await held).status_code == 200
                assert guard.active == 0 and guard.streams == 0 and not guard.uploading
                entered.clear()
                release.clear()

    asyncio.run(scenario())


def test_interrupted_multipart_closes_rolled_spools_before_returning() -> None:
    async def scenario() -> None:
        async def chunks() -> AsyncGenerator[bytes]:
            yield (
                b"--boundary\r\nContent-Disposition: form-data; "
                b'name="file"; filename="data.csv"\r\n\r\n'
            )
            for _ in range(32):
                yield b"x" * 65536
            raise OperationError("PAYLOAD_TOO_LARGE", "injected stream ceiling")

        parser = _Multipart(
            Headers({"Content-Type": "multipart/form-data; boundary=boundary"}),
            chunks(),
            max_files=1,
            max_fields=0,
        )
        with pytest.raises(OperationError, match="stream ceiling"):
            await parser.parse()
        assert len(parser._files_to_close_on_error) == 1
        assert all(spool.closed for spool in parser._files_to_close_on_error)

    asyncio.run(scenario())


@pytest.mark.parametrize("host", [None, ipaddress.IPv4Address(0).compressed, "::", "192.0.2.10"])
def test_effective_deployment_rejects_nonloopback_publishing(host: str | None) -> None:
    with pytest.raises(ValueError, match="loopback"):
        check(json.dumps({"services": {"api": {"ports": [{"host_ip": host}]}}}).encode())
    check(b'{"services":{"api":{"ports":[{"host_ip":"127.0.0.1"}]}}}')
    with pytest.raises(ValueError, match="Private"):
        check(b'{"services":{"postgres":{"ports":[{"host_ip":"127.0.0.1"}]}}}')


@pytest.mark.parametrize("archive", [False, True])
def test_disconnect_closes_verified_file_and_archive_resources(
    tmp_path: Path, archive: bool
) -> None:
    files = ManagedFiles(tmp_path, StorageLimits(min_free_bytes=0))
    reference = files.publish("datasets/disconnect.csv", (os.urandom(2 * 1024**2),), 3 * 1024**2)
    response = ArtifactResponse(
        downloads=(
            Download(
                files=files, reference=reference, filename="requests.csv", media_type="text/csv"
            ),
        ),
        archive=archive,
    )

    async def scenario() -> None:
        async def receive() -> dict[str, str]:
            return {"type": "http.disconnect"}

        async def send(message: Message) -> None:
            if message["type"] == "http.response.body":
                raise ClientDisconnect()

        await asyncio.wait_for(
            response(
                {"type": "http", "method": "GET", "asgi": {"spec_version": "2.4"}}, receive, send
            ),
            timeout=5,
        )
        assert response.files and all(stream.closed for stream in response.files)
        with pytest.raises(StopAsyncIteration):
            await anext(response.source)

    asyncio.run(scenario())


def test_materialization_admission_rejects_overlap_until_cancelled_work_finishes() -> None:
    async def scenario() -> None:
        admission = MaterializationAdmission()
        entered, release, settled = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def materialize() -> int:
            entered.set()
            await release.wait()
            settled.set()
            return 42

        task = asyncio.create_task(admission.run(materialize))
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(OperationError, match="capacity"):
            await admission.run(materialize)
        assert not settled.is_set()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert settled.is_set() and not admission.lock.locked()
        assert await admission.run(materialize) == 42

    asyncio.run(scenario())


def test_invalid_access_configuration_fails_before_serving() -> None:
    for create in (
        lambda: Settings(auth_enabled=True),
        lambda: Settings(allowed_hosts=("*",)),
        lambda: Settings(allowed_hosts=("bad\r\nhost",)),
        lambda: Settings(allowed_origins=("https://dashboard.test/path",)),
        lambda: Settings(allowed_origins=("https://user:pass@dashboard.test",)),
    ):
        with pytest.raises(OperationError, match="access policy"):
            create()


def test_signing_key_persists_and_rejects_unsafe_files(tmp_path: Path) -> None:
    state = tmp_path / "state"
    value = cursor_secret(state)
    assert len(value) == 32 and cursor_secret(state) == value
    key = state / "cursor.key"
    key.chmod(0o644)
    with pytest.raises(OperationError, match="permissions"):
        cursor_secret(state)
    key.unlink()
    key.symlink_to(tmp_path / "elsewhere")
    with pytest.raises(OSError):
        cursor_secret(state)

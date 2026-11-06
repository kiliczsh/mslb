import asyncio
from collections.abc import AsyncGenerator
from datetime import datetime
from pathlib import Path
from threading import Event
from uuid import UUID

import httpx
import pytest
from mslb_api.app import create_app
from mslb_api.settings import Settings
from mslb_application.event_feed import EventFeed
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_application.worker import Worker
from mslb_contracts.inputs import RequestInput
from mslb_contracts.queries import RunView
from mslb_persistence.reads import Reads
from sqlalchemy.exc import DBAPIError

from tests.integration.helpers import scratch


def test_invalid_and_chunked_oversized_uploads_leave_no_dataset_or_staging(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            inputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "inputs", StorageLimits(min_free_bytes=0)
            )
            outputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "outputs", StorageLimits(min_free_bytes=0)
            )
            app = create_app(
                Settings(allowed_hosts=("testserver",)),
                database,
                inputs,
                outputs,
                b"limit-check-signing-key" * 2,
            )
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://testserver/api/v1/"
                ) as client:
                    for files in (
                        {"file": ("bad.csv", b"private-input-must-not-be-echoed", "text/csv")},
                        {"other": ("bad.csv", b"invalid", "text/csv")},
                        [("file", ("a.csv", b"invalid")), ("file", ("b.csv", b"invalid"))],
                    ):
                        response = await client.post("datasets", files=files)
                        assert response.status_code == 422, response.text
                        assert "private-input-must-not-be-echoed" not in response.text
                    malformed = await client.post(
                        "datasets",
                        content=b"broken",
                        headers={"Content-Type": "multipart/form-data"},
                    )
                    assert malformed.status_code == 422

                    async def oversized() -> AsyncGenerator[bytes]:
                        yield (
                            b"--b\r\nContent-Disposition: form-data; "
                            b'name="file"; filename="data.csv"\r\n\r\n'
                        )
                        for _ in range(163):
                            yield b"x" * 65536
                        yield b"\r\n--b--\r\n"

                    response = await client.post(
                        "datasets",
                        content=oversized(),
                        headers={"Content-Type": "multipart/form-data; boundary=b"},
                    )
                    assert response.status_code == 413, response.text
                    assert (await client.get("datasets")).json()["items"] == []
                    assert (
                        await asyncio.to_thread(lambda: list((inputs.root / ".staging").iterdir()))
                        == []
                    )

    asyncio.run(scenario())


def test_dependency_error_envelope_redacts_sql_and_private_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def broken(
        self: Reads, after: tuple[datetime, UUID] | None = None, limit: int = 100
    ) -> tuple[RunView, ...]:
        raise DBAPIError("private-query-text", {}, OSError("private-database-error"), False)

    async def scenario() -> None:
        async with scratch() as database:
            inputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "inputs", StorageLimits(min_free_bytes=0)
            )
            outputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "outputs", StorageLimits(min_free_bytes=0)
            )
            app = create_app(
                Settings(allowed_hosts=("testserver",)),
                database,
                inputs,
                outputs,
                b"safe-error-signing-key" * 2,
            )
            async with app.router.lifespan_context(app):
                monkeypatch.setattr(Reads, "runs", broken)
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://testserver/api/v1/"
                ) as client:
                    response = await client.get("simulations")
                    assert response.status_code == 503 and response.headers["retry-after"] == "1"
                    assert response.json()["error"]["code"] == "RESOURCE_PRESSURE"
                    assert "private-" not in response.text
                    assert response.headers["x-request-id"] == response.json()["request_id"]

    asyncio.run(scenario())


def test_identical_http_retries_share_materialization_and_survive_owner_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = ManagedFiles.load_requests
    entered, release = Event(), Event()
    calls = 0

    def blocked(self: ManagedFiles, digest: str, size: int) -> tuple[RequestInput, ...]:
        nonlocal calls
        calls += 1
        entered.set()
        assert release.wait(10)
        return original(self, digest, size)

    async def scenario() -> None:
        async with scratch() as database:
            inputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "inputs", StorageLimits(min_free_bytes=0)
            )
            outputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "outputs", StorageLimits(min_free_bytes=0)
            )
            app = create_app(
                Settings(allowed_hosts=("testserver",)),
                database,
                inputs,
                outputs,
                b"retry-check-signing-key" * 2,
            )
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://testserver/api/v1/"
                ) as client:
                    dataset = await client.post(
                        "datasets",
                        files={
                            "file": (
                                "requests.csv",
                                b"t,request_id,work_units,mem_mb\n0,r,1,1\n",
                                "text/csv",
                            )
                        },
                    )
                    body = {"dataset_id": dataset.json()["id"]}
                    monkeypatch.setattr(ManagedFiles, "load_requests", blocked)
                    first = asyncio.create_task(
                        client.post("simulations", json=body, headers={"Idempotency-Key": "same"})
                    )
                    assert await asyncio.to_thread(entered.wait, 5)
                    second = asyncio.create_task(
                        client.post("simulations", json=body, headers={"Idempotency-Key": "same"})
                    )
                    await asyncio.sleep(0.03)
                    conflict = await client.post(
                        "simulations",
                        json=body | {"strategy": "round_robin"},
                        headers={"Idempotency-Key": "same"},
                    )
                    assert conflict.status_code == 409
                    rejected = await client.post(
                        "simulations", json=body, headers={"Idempotency-Key": "different"}
                    )
                    assert rejected.status_code == 429
                    first.cancel()
                    await asyncio.sleep(0.03)
                    assert not first.done() and calls == 1
                    release.set()
                    with pytest.raises(asyncio.CancelledError):
                        await first
                    duplicate = await second
                    assert duplicate.status_code == 202, duplicate.text
                    assert len(await Reads(database).runs()) == 1 and calls == 1

    try:
        asyncio.run(scenario())
    finally:
        release.set()


def test_live_sse_replay_and_slow_consumer_keep_worker_output_independent(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            inputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "inputs", StorageLimits(min_free_bytes=0)
            )
            outputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "outputs", StorageLimits(min_free_bytes=0)
            )
            app = create_app(
                Settings(allowed_hosts=("testserver",)),
                database,
                inputs,
                outputs,
                b"sse-check-signing-key" * 2,
            )
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://testserver/api/v1/"
                ) as client:
                    source = (
                        "t,request_id,work_units,mem_mb\n"
                        + "".join(f"0,r{i:04d},1,1\n" for i in range(251))
                    ).encode()
                    dataset = await client.post(
                        "datasets", files={"file": ("requests.csv", source, "text/csv")}
                    )
                    run = await client.post(
                        "simulations", json={"dataset_id": dataset.json()["id"]}
                    )
                    run_id = run.json()["run_id"]
                    streaming = asyncio.create_task(client.get(f"simulations/{run_id}/stream"))
                    await asyncio.sleep(0.1)
                    assert not streaming.done()
                    assert await Worker(database, inputs, outputs).run_once()
                    response = await asyncio.wait_for(streaming, 10)
                    sequences = [
                        int(line[4:])
                        for line in response.text.splitlines()
                        if line.startswith("id: ")
                    ]
                    assert sequences == list(range(1, 754))
                    detail = await Reads(database).run(UUID(run_id))
                    feed = EventFeed(database, detail.id)
                    await asyncio.wait_for(feed.produce(detail), 5)
                    assert feed.overflow and feed.queued_bytes <= 64 * 1024
                    assert (await Reads(database).run(detail.id)).status == "completed"

    asyncio.run(scenario())

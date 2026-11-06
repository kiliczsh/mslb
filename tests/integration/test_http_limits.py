import asyncio
from collections.abc import AsyncGenerator
from datetime import datetime
from http import HTTPStatus
from pathlib import Path
from threading import Event
from uuid import UUID

import httpx
import pytest
from mslb_api.app import create_app
from mslb_api.settings import Settings
from mslb_api.streams import SEND_TIMEOUT_SECONDS, EventResponse
from mslb_application.event_feed import EventFeed
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_application.worker import Worker
from mslb_contracts.durability import PersistedEvent
from mslb_contracts.inputs import RequestInput
from mslb_contracts.queries import RunView
from mslb_persistence.reads import Reads
from sqlalchemy.exc import DBAPIError
from starlette.types import Receive, Scope, Send

from tests.integration.helpers import scratch
from tests.integration.streaming_transport import SlowHttpTransport


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
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://testserver/api/v1/"
                ) as client,
            ):
                for files in (
                    {"file": ("bad.csv", b"private-input-must-not-be-echoed", "text/csv")},
                    {"other": ("bad.csv", b"invalid", "text/csv")},
                    [("file", ("a.csv", b"invalid")), ("file", ("b.csv", b"invalid"))],
                ):
                    response = await client.post("datasets", files=files)
                    assert response.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value, (
                        response.text
                    )
                    assert "private-input-must-not-be-echoed" not in response.text
                malformed = await client.post(
                    "datasets",
                    content=b"broken",
                    headers={"Content-Type": "multipart/form-data"},
                )
                assert malformed.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value

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
                assert response.status_code == HTTPStatus.CONTENT_TOO_LARGE.value, response.text
                assert (await client.get("datasets")).json()["items"] == []
                assert (
                    await asyncio.to_thread(lambda: list((inputs.root / ".staging").iterdir()))
                    == []
                )

    asyncio.run(scenario())


def test_dependency_error_envelope_redacts_sql_and_private_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
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
                    assert (
                        response.status_code == HTTPStatus.SERVICE_UNAVAILABLE.value
                        and response.headers["retry-after"] == "1"
                    )
                    assert response.json()["error"]["code"] == "RESOURCE_PRESSURE"
                    assert "private-" not in response.text
                    assert response.headers["x-request-id"] == response.json()["request_id"]
                    records = [
                        record
                        for record in caplog.records
                        if record.name == "mslb_application.diagnostics"
                    ]
                    assert len(records) == 1 and records[0].exc_info is None
                    assert "private-" not in records[0].getMessage()
                    assert response.json()["request_id"] in records[0].getMessage()
                    assert (
                        "source=rest code=RESOURCE_PRESSURE error_type=DBAPIError"
                        in records[0].getMessage()
                    )

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
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://testserver/api/v1/"
                ) as client,
            ):
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
                assert conflict.status_code == HTTPStatus.CONFLICT.value
                rejected = await client.post(
                    "simulations", json=body, headers={"Idempotency-Key": "different"}
                )
                assert rejected.status_code == HTTPStatus.TOO_MANY_REQUESTS.value
                first.cancel()
                await asyncio.sleep(0.03)
                assert not first.done() and calls == 1
                release.set()
                with pytest.raises(asyncio.CancelledError):
                    await first
                duplicate = await second
                assert duplicate.status_code == HTTPStatus.ACCEPTED.value, duplicate.text
                assert len(await Reads(database).runs()) == 1 and calls == 1

    try:
        asyncio.run(scenario())
    finally:
        release.set()


@pytest.mark.parametrize(
    ("close_kind", "identifier_padding"),
    [
        pytest.param("send_deadline", 0, id="send_deadline-frame_budget"),
        pytest.param("client_disconnect", 0, id="client_disconnect-frame_budget"),
        pytest.param("send_deadline", 90, id="send_deadline-byte_budget"),
        pytest.param("client_disconnect", 90, id="client_disconnect-byte_budget"),
    ],
)
def test_blocked_http_sse_consumer_releases_admission_and_replays_complete_durable_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, close_kind: str, identifier_padding: int
) -> None:
    release_validation = Event()
    overflow = asyncio.Event()
    feeds: list[EventFeed] = []
    responses: list[EventResponse] = []
    peak_frames, peak_bytes = 0, 0
    original_offer, original_response = EventFeed.offer, EventResponse.__call__

    def observed_offer(self: EventFeed, frame: bytes) -> bool:
        nonlocal peak_frames, peak_bytes
        if not feeds:
            feeds.append(self)
        accepted = original_offer(self=self, frame=frame)
        if self is feeds[0]:
            peak_frames = max(peak_frames, self.queue.qsize())
            peak_bytes = max(peak_bytes, self.queued_bytes)
            if self.overflow:
                overflow.set()
        return accepted

    async def observed_response(
        self: EventResponse, scope: Scope, receive: Receive, send: Send
    ) -> None:
        responses.append(self)
        await original_response(self=self, scope=scope, receive=receive, send=send)

    monkeypatch.setattr(target=EventFeed, name="offer", value=observed_offer)
    monkeypatch.setattr(target=EventResponse, name="__call__", value=observed_response)

    async def scenario() -> None:
        async with scratch() as database:
            inputs = await asyncio.to_thread(
                ManagedFiles, root=tmp_path / "inputs", limits=StorageLimits(min_free_bytes=0)
            )
            outputs = await asyncio.to_thread(
                ManagedFiles, root=tmp_path / "outputs", limits=StorageLimits(min_free_bytes=0)
            )
            app = create_app(
                settings=Settings(allowed_hosts=("testserver",), stream_limit=1, request_limit=4),
                database=database,
                inputs=inputs,
                outputs=outputs,
                signing_key=b"sse-check-signing-key" * 2,
            )
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://testserver/api/v1/"
                ) as client,
            ):
                source = (
                    "t,request_id,work_units,mem_mb\n"
                    + "".join(f"0,r{i:04d}{'x' * identifier_padding},1,1\n" for i in range(251))
                ).encode()
                dataset = await client.post(
                    url="datasets", files={"file": ("requests.csv", source, "text/csv")}
                )
                assert dataset.status_code == HTTPStatus.CREATED.value, dataset.text
                run = await client.post(
                    url="simulations", json={"dataset_id": dataset.json()["id"]}
                )
                assert run.status_code == HTTPStatus.ACCEPTED.value, run.text
                run_id = run.json()["run_id"]

                def hold_terminal(phase: str, lease: object, tick: int) -> None:
                    if phase == "terminal_committed":
                        assert release_validation.wait(timeout=12), (
                            "Test did not release worker validation"
                        )

                transport = SlowHttpTransport(app=app)
                async with (
                    httpx.AsyncClient(
                        transport=transport, base_url="http://testserver/api/v1/"
                    ) as slow,
                    slow.stream(method="GET", url=f"simulations/{run_id}/stream") as response,
                ):
                    assert response.status_code == HTTPStatus.OK.value
                    assert response.headers["content-type"].startswith("text/event-stream")
                    worker = asyncio.create_task(
                        coro=Worker(
                            database=database,
                            inputs=inputs,
                            outputs=outputs,
                            observer=hold_terminal,
                        ).run_once()
                    )
                    lines = response.aiter_lines()
                    assert isinstance(lines, AsyncGenerator)
                    received: list[PersistedEvent] = []
                    last_received_id = 0
                    try:
                        async with asyncio.timeout(delay=6):
                            async for line in lines:
                                if line.startswith("id: "):
                                    last_received_id = int(line[4:])
                                if line.startswith("data: ") and '"sequence":' in line:
                                    received.append(
                                        PersistedEvent.model_validate_json(json_data=line[6:])
                                    )
                                    assert received[-1].sequence == last_received_id
                                    break
                        assert [event.sequence for event in received] == [1]
                        assert transport.sending.is_set() and not transport.finished.is_set()
                        await asyncio.wait_for(fut=overflow.wait(), timeout=3)
                        assert 0 < peak_frames <= 500
                        assert 0 < peak_bytes <= 64 * 1024
                        if identifier_padding:
                            assert peak_frames < 500, (
                                "Long frames did not exercise the byte ceiling"
                            )
                            assert peak_bytes > 60 * 1024
                        else:
                            assert peak_frames == 500, (
                                "Short frames did not exercise the frame ceiling"
                            )
                        assert transport.peak_frames == 1
                        denied = await client.get(url=f"simulations/{run_id}/stream")
                        assert denied.status_code == HTTPStatus.TOO_MANY_REQUESTS.value, (
                            "Blocked connection did not retain stream admission"
                        )
                        assert (await client.get(url=f"simulations/{run_id}")).json()[
                            "status"
                        ] == "running"
                        release_validation.set()
                        assert await asyncio.wait_for(fut=worker, timeout=5)
                        assert (
                            await Reads(database=database).run(run_id=UUID(hex=run_id))
                        ).status == "completed"
                        assert not transport.finished.is_set(), (
                            "Worker completed only after the blocked stream ended"
                        )
                        if close_kind == "client_disconnect":
                            await response.aclose()
                        try:
                            await asyncio.wait_for(
                                fut=transport.finished.wait(),
                                timeout=SEND_TIMEOUT_SECONDS + 2,
                            )
                        except TimeoutError:
                            raise AssertionError(
                                "Blocked HTTP SSE connection missed its cleanup deadline"
                            ) from None
                        assert transport.task is not None and transport.task.done()
                        await transport.task
                        assert feeds[0].closed.is_set()
                        # Retain the response so GC cannot hide missing cleanup.
                        with pytest.raises(expected_exception=StopAsyncIteration):
                            await asyncio.wait_for(fut=anext(responses[0].source), timeout=1)
                    finally:
                        release_validation.set()
                        await lines.aclose()
                        await asyncio.wait_for(fut=worker, timeout=8)

                replay = await client.get(
                    url=f"simulations/{run_id}/stream",
                    headers={"Last-Event-ID": str(last_received_id)},
                )
                assert replay.status_code == HTTPStatus.OK.value, (
                    "Stream admission was not released after cleanup"
                )
                replayed = [
                    PersistedEvent.model_validate_json(json_data=line[6:])
                    for line in replay.text.splitlines()
                    if line.startswith("data: ") and '"sequence":' in line
                ]
                ids = [event.sequence for event in (*received, *replayed)]
                assert ids == list(range(1, 754)), (
                    "Reconnect lost, duplicated or reordered a durable event"
                )
                assert [
                    int(line[4:]) for line in replay.text.splitlines() if line.startswith("id: ")
                ] == list(range(last_received_id + 1, 754))
                assert "event: status\n" in replay.text and '"status":"completed"' in replay.text

    try:
        asyncio.run(main=scenario())
    finally:
        release_validation.set()

import io
import json
from threading import Event
from typing import override

import pytest
from mslb_application.cursors import Cursors, Position
from mslb_application.errors import OperationError
from mslb_application.metric_queries import metric_report, server_metric_page


def test_cursor_authenticates_collection_filter_position_and_lifetime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(target="mslb_application.cursors.time.time", name=lambda: 1000)
    cursors = Cursors(secret=b"local-test-cursor-key" * 2)
    token = cursors.encode(context=["events", "run", 3, 8], position=Position(sequence=42))
    assert cursors.decode(context=["events", "run", 3, 8], token=token).sequence == 42
    for context, changed in [
        (["events", "other", 3, 8], token),
        (["events", "run", 4, 8], token),
        (["events", "run", 3, 8], "A" + token[1:]),
        (["events", "run", 3, 8], "!"),
        (["events", "run", 3, 8], "x" * 2049),
    ]:
        with pytest.raises(expected_exception=OperationError, match="cursor"):
            cursors.decode(context=context, token=changed)
    monkeypatch.setattr(
        target="mslb_application.cursors.time.time", name=lambda: 1000 + 24 * 60 * 60
    )
    with pytest.raises(expected_exception=OperationError, match="expired"):
        cursors.decode(context=["events", "run", 3, 8], token=token)


def test_metric_selection_streams_large_server_map_in_bounded_reads() -> None:
    class BoundedReads(io.BytesIO):
        @override
        def read(self, size: int | None = -1, /) -> bytes:
            assert size is not None and 0 <= size <= 65536
            return super().read(size)

    data = {
        "finished": 10_000,
        "cluster": {"cpu_consumed": {"numerator": "1", "denominator": "3"}},
        "servers": {f"server-{i:05d}": {"finished": i} for i in range(10_000)},
    }
    stream = BoundedReads(initial_bytes=json.dumps(obj=data, sort_keys=True).encode())
    summary, selected = metric_report(stream=stream, server_ids=("server-09999",))
    assert summary == {"finished": 10_000, "cluster": data["cluster"]}
    assert selected == {"server-09999": {"finished": 9999}}
    stream.seek(0)
    page = server_metric_page(stream=stream, after="server-09997", limit=100)
    assert [row.id for row in page] == ["server-09998", "server-09999"]
    stream.seek(0)
    with pytest.raises(expected_exception=OperationError, match="not in this run"):
        metric_report(stream=stream, server_ids=("unknown",))


def test_cancelled_file_query_keeps_admission_until_thread_exits() -> None:
    import asyncio

    from mslb_application.queries import Queries

    async def scenario() -> None:
        entered, release, next_entered = Event(), Event(), Event()

        def first() -> None:
            entered.set()
            assert release.wait(timeout=5)

        service = object.__new__(Queries)
        service.file_queries = asyncio.Semaphore(value=1)
        # Exercise the admission boundary without a database or filesystem dependency.
        task = asyncio.create_task(coro=service._file_call(operation=first))
        assert await asyncio.to_thread(entered.wait, timeout=5)
        task.cancel()
        following = asyncio.create_task(coro=service._file_call(operation=next_entered.set))
        await asyncio.sleep(delay=0.03)
        task.cancel()
        await asyncio.sleep(delay=0.03)
        assert not next_entered.is_set()
        release.set()
        with pytest.raises(expected_exception=asyncio.CancelledError):
            await task
        await following
        assert next_entered.is_set()
        with pytest.raises(expected_exception=OperationError, match="unavailable or invalid"):
            await service._file_call(
                operation=lambda: metric_report(
                    stream=io.BytesIO(initial_bytes=b"{"), server_ids=()
                )
            )

    asyncio.run(main=scenario())

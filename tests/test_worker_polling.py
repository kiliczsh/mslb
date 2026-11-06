"""Outage diagnostics cannot flood polling, hide fail-fast errors or swallow cancellation."""

import asyncio
from threading import Event
from unittest.mock import AsyncMock, Mock

import pytest
from mslb_application.worker import Worker
from mslb_worker.polling import poll
from mslb_worker.redis_notifications import RedisHints


def test_polling_warns_once_per_outage_and_resets_after_success(
    *, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        worker, hints, shutdown = Mock(spec=Worker), Mock(spec=RedisHints), Event()
        outcomes = iter([False, False, True, False, True])

        async def attempt(*, shutdown: Event) -> bool:
            if not next(outcomes):
                raise OSError("private-dependency-url")
            if worker.run_once.await_count == 5:
                shutdown.set()
            return False

        worker.run_once = AsyncMock(side_effect=attempt)
        delay = AsyncMock()
        monkeypatch.setattr(target=asyncio, name="sleep", value=delay)
        await poll(worker=worker, hints=hints, shutdown=shutdown, once=False)
        assert worker.run_once.await_count == 5 and delay.await_count == 3
        assert hints.wait.await_count == 2
        assert all(call.kwargs == {"delay": 1} for call in delay.await_args_list)

    asyncio.run(main=scenario())
    assert len(caplog.records) == 2
    for record in caplog.records:
        assert "source=worker_poll code=RESOURCE_PRESSURE" in record.getMessage()
        assert "private-" not in record.getMessage() and record.exc_info is None


@pytest.mark.parametrize(
    argnames="error", argvalues=[OSError("private-url"), asyncio.CancelledError()]
)
def test_one_shot_failures_and_cancellation_propagate_without_retry(
    *, error: BaseException, caplog: pytest.LogCaptureFixture
) -> None:
    async def scenario() -> None:
        worker, hints = Mock(spec=Worker), Mock(spec=RedisHints)
        worker.run_once = AsyncMock(side_effect=error)
        with pytest.raises(expected_exception=type(error)) as raised:
            await poll(worker=worker, hints=hints, shutdown=Event(), once=True)
        assert raised.value is error and worker.run_once.await_count == 1
        hints.wait.assert_not_awaited()

    asyncio.run(main=scenario())
    assert not caplog.records

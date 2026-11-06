import asyncio
from uuid import UUID, uuid4

import pytest
from mslb_application.notifications import OutboxNotifications
from mslb_worker.redis_notifications import RedisHints
from redis.exceptions import ConnectionError as RedisConnectionError


class Outbox:
    def __init__(self) -> None:
        self.pending: list[tuple[UUID, UUID]] = [(uuid4(), uuid4()) for _ in range(25)]
        self.delivered: list[UUID] = []
        self.fail_ack = False
        self.prunes = 0

    async def pending_outbox(self, *, limit: int) -> tuple[tuple[UUID, UUID], ...]:
        assert limit == 20
        return tuple(self.pending[:limit])

    async def mark_outbox_delivered(self, *, message_id: UUID) -> None:
        if self.fail_ack:
            raise OSError("injected acknowledgment failure")
        self.delivered.append(message_id)
        self.pending = [row for row in self.pending if row[0] != message_id]

    async def prune_outbox(self) -> None:
        self.prunes += 1


class Hints:
    def __init__(self, *, available: bool = True) -> None:
        self.available = available
        self.published: list[UUID] = []

    async def publish(self, *, run_id: UUID) -> bool:
        if self.available:
            self.published.append(run_id)
        return self.available

    async def wait(self) -> None:
        return


def test_bounded_outbox_page_acknowledges_only_accepted_uuid_hints() -> None:
    async def scenario() -> None:
        jobs, hints = Outbox(), Hints()
        notifier = OutboxNotifications(jobs=jobs, hints=hints)
        expected = [run_id for _, run_id in jobs.pending[:20]]
        assert await notifier.dispatch_once() == 20
        assert hints.published == expected
        assert len(jobs.pending) == 5
        hints.available = False
        assert await notifier.dispatch_once() == 0
        assert len(jobs.pending) == 5
        hints.available = True
        assert await notifier.dispatch_once() == 5
        assert jobs.pending == []

    asyncio.run(main=scenario())


def test_acknowledgment_failure_leaves_retryable_hint_and_shutdown_is_prompt() -> None:
    async def scenario() -> None:
        jobs, hints = Outbox(), Hints()
        notifier = OutboxNotifications(jobs=jobs, hints=hints)
        jobs.fail_ack = True
        with pytest.raises(expected_exception=OSError, match="acknowledgment"):
            await notifier.dispatch_once()
        assert len(jobs.pending) == 25
        first = hints.published[0]
        jobs.fail_ack = False
        assert await notifier.dispatch_once() == 20
        assert hints.published[1] == first
        done = asyncio.Event()
        done.set()
        await asyncio.wait_for(fut=notifier.run(done=done), timeout=0.1)

    asyncio.run(main=scenario())


def test_dispatch_loop_prunes_during_redis_outage_and_stops_without_retry_spin() -> None:
    async def scenario() -> None:
        jobs, hints, done = Outbox(), Hints(available=False), asyncio.Event()
        notifier = OutboxNotifications(jobs=jobs, hints=hints)
        task = asyncio.create_task(coro=notifier.run(done=done))
        await asyncio.sleep(delay=0)
        assert jobs.prunes == 1
        assert len(jobs.pending) == 25
        done.set()
        await asyncio.wait_for(fut=task, timeout=0.1)

    asyncio.run(main=scenario())


class FailedRedis:
    async def eval(self, *args: object) -> int:
        raise RedisConnectionError("injected unavailable Redis")

    async def blpop(self, **options: object) -> None:
        assert options == {"keys": ["mslb:wakeups:mslb_test_notifications"], "timeout": 0.1}
        raise RedisConnectionError("injected unavailable Redis")


class BlockedRedis:
    async def eval(self, *args: object) -> int:
        await asyncio.Event().wait()
        return 1

    async def blpop(self, **options: object) -> None:
        assert options == {"keys": ["mslb:wakeups:mslb_test_notifications"], "timeout": 0.1}
        await asyncio.Event().wait()


@pytest.mark.parametrize(argnames="client", argvalues=[FailedRedis(), BlockedRedis()])
def test_unavailable_or_stalled_redis_has_finite_delivery_and_db_poll_wait(
    client: FailedRedis | BlockedRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        hints = RedisHints(url="redis://localhost:1/0", namespace="mslb_test_notifications")
        original = hints.client
        monkeypatch.setattr(target=hints, name="client", value=client)
        try:
            assert not await asyncio.wait_for(fut=hints.publish(run_id=uuid4()), timeout=0.5)
            await asyncio.wait_for(fut=hints.wait(), timeout=0.5)
        finally:
            await original.aclose()

    asyncio.run(main=scenario())


def test_hint_namespace_rejects_arbitrary_keys() -> None:
    with pytest.raises(expected_exception=ValueError, match="namespace"):
        RedisHints(url="redis://localhost:1/0", namespace="other:project")

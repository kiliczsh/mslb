"""Real project Redis bounds and PostgreSQL authority under lost/unavailable hints."""

import asyncio
import os
from uuid import uuid4

from mslb_application.notifications import OutboxNotifications
from mslb_persistence.jobs import Jobs
from mslb_worker.redis_notifications import RedisHints

from tests.integration.helpers import scratch
from tests.integration.test_persistence import prepare


def test_redis_hints_are_uuid_only_bounded_expiring_and_namespaced() -> None:
    async def scenario() -> None:
        hints = RedisHints(url=os.environ["TEST_REDIS_URL"], namespace="mslb_test_" + uuid4().hex)
        try:
            expected = uuid4()
            for _ in range(140):
                assert await hints.publish(run_id=expected)
            assert await hints.client.llen(name=hints.key) == 128
            assert 0 < await hints.client.ttl(name=hints.key) <= 60
            assert await hints.client.lindex(name=hints.key, index=0) == str(expected).encode()
            await asyncio.wait_for(fut=hints.wait(), timeout=0.5)
            assert await hints.client.llen(name=hints.key) == 127
            # Eviction/restart is represented by deleting only this test's exact
            # disposable key. Durable state never lives in this namespace.
            await hints.client.delete(hints.key)
            await asyncio.wait_for(fut=hints.wait(), timeout=0.5)
            assert await hints.publish(run_id=expected)
        finally:
            await hints.client.delete(hints.key)
            await hints.close()

    asyncio.run(main=scenario())


def test_redis_outage_and_lost_delivered_hints_cannot_prevent_durable_claims() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            first = await prepared.enqueue()
            unavailable = RedisHints(url="redis://127.0.0.1:1/0", namespace=database.schema)
            recovered = RedisHints(url=os.environ["TEST_REDIS_URL"], namespace=database.schema)
            jobs = Jobs(database=database)
            notifier = OutboxNotifications(jobs=jobs, hints=unavailable)
            try:
                assert await notifier.dispatch_once() == 0
                assert len(await jobs.pending_outbox()) == 1
                # The ordinary DB claim succeeds without consuming any Redis hint.
                lease = await jobs.claim(owner="db-fallback")
                assert lease is not None and lease.run_id == first.id
                await jobs.fail(lease=lease, error_code="RESOURCE_PRESSURE")
                second = await prepared.enqueue()
                notifier = OutboxNotifications(jobs=jobs, hints=recovered)
                assert await notifier.dispatch_once() == 2
                assert await jobs.pending_outbox() == ()
                await recovered.client.delete(recovered.key)
                next_lease = await jobs.claim(owner="after-hint-loss")
                assert next_lease is not None and next_lease.run_id == second.id
                await jobs.fail(lease=next_lease, error_code="RESOURCE_PRESSURE")
                assert (await jobs.get(run_id=first.id)).status == "failed"
                assert (await jobs.get(run_id=second.id)).status == "failed"
            finally:
                await recovered.client.delete(recovered.key)
                await unavailable.close()
                await recovered.close()

    asyncio.run(main=scenario())

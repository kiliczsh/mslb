"""Actual PostgreSQL ownership around ambiguous/foreign daemon observations."""

import asyncio
from typing import override

import httpx
import pytest
from mslb_contracts.docker_runs import CreationIntent
from mslb_controller.engine import (
    ContainerInfo,
    Created,
    Engine,
    EngineError,
    ImageInfo,
    NetworkInfo,
)
from mslb_controller.lifecycle import Lifecycle
from mslb_controller.routing import BackendHttp
from mslb_persistence.database import StoreError
from mslb_persistence.docker_records import DeploymentRecord

from tests.integration.helpers import scratch
from tests.integration.test_docker_control import claim, expire, prepared
from tests.test_docker_engine import fixture


class ObservedEngine(Engine):
    def __init__(self, intent: CreationIntent) -> None:
        super().__init__(httpx.MockTransport(lambda _request: httpx.Response(500)))
        _intent, self.approved_image, self.approved_network, self.info = fixture(intent)
        self.absent = False
        self.image_absent = False
        self.create_error: EngineError | None = None
        self.actions: list[str] = []

    @override
    async def image(self, image_id: str | None = None) -> ImageInfo:
        if self.image_absent:
            raise EngineError("ENGINE_STATUS", 404)
        return self.approved_image

    @override
    async def network(self) -> NetworkInfo:
        return self.approved_network

    @override
    async def inspect(self, reference: str) -> ContainerInfo:
        if self.absent:
            raise EngineError("ENGINE_STATUS", 404)
        return self.info

    @override
    async def create(self, intent: CreationIntent) -> Created:
        self.actions.append("create")
        if self.create_error is not None:
            raise self.create_error
        return Created(Id=self.info.Id)

    @override
    async def stop(self, container_id: str) -> None:
        self.actions.append("stop:" + container_id)
        self.info = self.info.model_copy(update={"State": {"Running": False}})

    @override
    async def remove(self, container_id: str) -> None:
        self.actions.append("remove:" + container_id)
        self.absent = True


def test_unknown_create_adopts_exact_definition_under_replacement_cleanup_only() -> None:
    async def run() -> None:
        async with scratch() as database:
            data = await prepared(database)
            await data.enqueue()
            old = await claim(data)
            record = await data.resources.prepare_dynamic(old)
            intent = await data.resources.authorize_create(old, record.id)
            await data.resources.transition(old, record.id, "uncertain")
            engine, http = ObservedEngine(intent), BackendHttp()
            lifecycle = Lifecycle(database, engine, http)
            try:
                engine.absent = True
                with pytest.raises(EngineError):
                    await lifecycle.cleanup(old, record.id)
                assert (await data.resources.get(old, record.id)).state == "unresolved"
                assert engine.actions == []
                await expire(database)
                recovery = await claim(data, "recovery")
                assert recovery.phase == "cleanup"
                engine.absent = False
                engine.info = engine.info.model_copy(update={"State": {"Running": True}})
                with pytest.raises(StoreError, match="current"):
                    await lifecycle.cleanup(old, record.id)
                resolved = await lifecycle.cleanup(recovery, record.id)
                assert resolved.state == "removed" and resolved.container_id == engine.info.Id
                assert engine.actions == ["stop:" + engine.info.Id, "remove:" + engine.info.Id]
                await data.control.finish_cleanup(recovery)
            finally:
                await engine.close()
                await http.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "status,state",
    [(400, "abandoned"), (409, "unresolved"), (500, "unresolved"), (None, "unresolved")],
)
def test_create_receipts_do_not_retry_or_release_uncertain_capacity(
    monkeypatch: pytest.MonkeyPatch, status: int | None, state: str
) -> None:
    async def run() -> None:
        async with scratch() as database:
            data = await prepared(database)
            await data.enqueue()
            lease = await claim(data)
            record = await data.resources.prepare_dynamic(lease)
            assert isinstance(record.definition, CreationIntent)
            engine, http = ObservedEngine(record.definition), BackendHttp()
            lifecycle = Lifecycle(database, engine, http)

            async def enrolled() -> DeploymentRecord:
                return await data.catalog.deployment(record.definition.deployment_id)

            monkeypatch.setattr(lifecycle, "enroll", enrolled)
            engine.create_error = EngineError(
                "ENGINE_STATUS" if status is not None else "ENGINE_TIMEOUT", status
            )
            try:
                with pytest.raises(EngineError):
                    await lifecycle.create(lease, record.id)
                assert (await data.resources.get(lease, record.id)).state == state
                with pytest.raises(StoreError, match="authorized"):
                    await lifecycle.create(lease, record.id)
                assert engine.actions == ["create"]
            finally:
                await engine.close()
                await http.close()

    asyncio.run(run())


def test_labels_cannot_authorize_foreign_configuration_or_false_absence() -> None:
    async def run() -> None:
        async with scratch() as database:
            data = await prepared(database)
            await data.enqueue()
            lease = await claim(data)
            record = await data.resources.prepare_dynamic(lease)
            intent = await data.resources.authorize_create(lease, record.id)
            engine, http = ObservedEngine(intent), BackendHttp()
            lifecycle = Lifecycle(database, engine, http)
            try:
                original = engine.info
                engine.info = original.model_copy(
                    update={"HostConfig": original.HostConfig | {"Privileged": True}}
                )
                with pytest.raises(EngineError, match="OWNERSHIP_CONFLICT"):
                    await lifecycle.cleanup(lease, record.id)
                assert (await data.resources.get(lease, record.id)).container_id is None
                assert engine.actions == []
                engine.info = original
                await lifecycle.observe(lease, record.id)
                engine.image_absent = True
                with pytest.raises(EngineError) as error:
                    await lifecycle.cleanup(lease, record.id)
                assert error.value.status is None
                assert (await data.resources.get(lease, record.id)).state == "observed"
                assert engine.actions == []
                engine.absent = True
                assert (await lifecycle.cleanup(lease, record.id)).state == "removed"
                assert engine.actions == []
            finally:
                await engine.close()
                await http.close()

    asyncio.run(run())

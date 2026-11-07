"""Actual PostgreSQL ownership around ambiguous/foreign daemon observations."""

import asyncio
from http import HTTPStatus
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
from tests.unit.platform.test_docker_engine import fixture


class ObservedEngine(Engine):
    def __init__(self, intent: CreationIntent) -> None:
        super().__init__(
            transport=httpx.MockTransport(
                handler=lambda _request: httpx.Response(
                    status_code=HTTPStatus.INTERNAL_SERVER_ERROR.value
                )
            )
        )
        _intent, self.approved_image, self.approved_network, self.info = fixture(intent=intent)
        self.absent = False
        self.image_absent = False
        self.create_error: EngineError | None = None
        self.actions: list[str] = []

    @override
    async def image(self, image_id: str | None = None) -> ImageInfo:
        if self.image_absent:
            raise EngineError(code="ENGINE_STATUS", status=HTTPStatus.NOT_FOUND.value)
        return self.approved_image

    @override
    async def network(self) -> NetworkInfo:
        return self.approved_network

    @override
    async def inspect(self, reference: str) -> ContainerInfo:
        if self.absent:
            raise EngineError(code="ENGINE_STATUS", status=HTTPStatus.NOT_FOUND.value)
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
            data = await prepared(database=database)
            await data.enqueue()
            old = await claim(data=data)
            record = await data.resources.prepare_dynamic(lease=old)
            intent = await data.resources.authorize_create(lease=old, resource_id=record.id)
            await data.resources.transition(lease=old, resource_id=record.id, action="uncertain")
            engine, http = ObservedEngine(intent=intent), BackendHttp()
            lifecycle = Lifecycle(database=database, engine=engine, http=http)
            try:
                engine.absent = True
                with pytest.raises(expected_exception=EngineError):
                    await lifecycle.cleanup(lease=old, identifier=record.id)
                assert (
                    await data.resources.get(lease=old, resource_id=record.id)
                ).state == "unresolved"
                assert engine.actions == []
                await expire(database=database)
                recovery = await claim(data=data, owner="recovery")
                assert recovery.phase == "cleanup"
                engine.absent = False
                engine.info = engine.info.model_copy(update={"State": {"Running": True}})
                with pytest.raises(expected_exception=StoreError, match="current"):
                    await lifecycle.cleanup(lease=old, identifier=record.id)
                resolved = await lifecycle.cleanup(lease=recovery, identifier=record.id)
                assert resolved.state == "removed" and resolved.container_id == engine.info.Id
                assert engine.actions == ["stop:" + engine.info.Id, "remove:" + engine.info.Id]
                await data.control.finish_cleanup(lease=recovery)
            finally:
                await engine.close()
                await http.close()

    asyncio.run(main=run())


@pytest.mark.parametrize(
    argnames="status,state",
    argvalues=[
        (HTTPStatus.BAD_REQUEST.value, "abandoned"),
        (HTTPStatus.CONFLICT.value, "unresolved"),
        (HTTPStatus.INTERNAL_SERVER_ERROR.value, "unresolved"),
        (None, "unresolved"),
    ],
)
def test_create_receipts_do_not_retry_or_release_uncertain_capacity(
    monkeypatch: pytest.MonkeyPatch, status: int | None, state: str
) -> None:
    async def run() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            await data.enqueue()
            lease = await claim(data=data)
            record = await data.resources.prepare_dynamic(lease=lease)
            assert isinstance(record.definition, CreationIntent)
            engine, http = ObservedEngine(intent=record.definition), BackendHttp()
            lifecycle = Lifecycle(database=database, engine=engine, http=http)

            async def enrolled() -> DeploymentRecord:
                return await data.catalog.deployment(deployment_id=record.definition.deployment_id)

            monkeypatch.setattr(target=lifecycle, name="enroll", value=enrolled)
            engine.create_error = EngineError(
                code="ENGINE_STATUS" if status is not None else "ENGINE_TIMEOUT", status=status
            )
            try:
                with pytest.raises(expected_exception=EngineError):
                    await lifecycle.create(lease=lease, identifier=record.id)
                assert (await data.resources.get(lease=lease, resource_id=record.id)).state == state
                with pytest.raises(expected_exception=StoreError, match="authorized"):
                    await lifecycle.create(lease=lease, identifier=record.id)
                assert engine.actions == ["create"]
            finally:
                await engine.close()
                await http.close()

    asyncio.run(main=run())


def test_labels_cannot_authorize_foreign_configuration_or_false_absence() -> None:
    async def run() -> None:
        async with scratch() as database:
            data = await prepared(database=database)
            await data.enqueue()
            lease = await claim(data=data)
            record = await data.resources.prepare_dynamic(lease=lease)
            intent = await data.resources.authorize_create(lease=lease, resource_id=record.id)
            engine, http = ObservedEngine(intent=intent), BackendHttp()
            lifecycle = Lifecycle(database=database, engine=engine, http=http)
            try:
                original = engine.info
                engine.info = original.model_copy(
                    update={"HostConfig": original.HostConfig | {"Privileged": True}}
                )
                with pytest.raises(expected_exception=EngineError, match="OWNERSHIP_CONFLICT"):
                    await lifecycle.cleanup(lease=lease, identifier=record.id)
                assert (
                    await data.resources.get(lease=lease, resource_id=record.id)
                ).container_id is None
                assert engine.actions == []
                engine.info = original
                await lifecycle.observe(lease=lease, identifier=record.id)
                engine.image_absent = True
                with pytest.raises(expected_exception=EngineError) as error:
                    await lifecycle.cleanup(lease=lease, identifier=record.id)
                assert error.value.status is None
                assert (
                    await data.resources.get(lease=lease, resource_id=record.id)
                ).state == "observed"
                assert engine.actions == []
                engine.absent = True
                assert (
                    await lifecycle.cleanup(lease=lease, identifier=record.id)
                ).state == "removed"
                assert engine.actions == []
            finally:
                await engine.close()
                await http.close()

    asyncio.run(main=run())

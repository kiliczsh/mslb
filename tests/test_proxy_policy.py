"""Credential, HTTP admission and effective deployment boundary without Docker access."""

import asyncio
import json
from http import HTTPStatus

import httpx
import pytest
from mslb_controller.engine import Engine
from mslb_controller.proxy import create_app
from mslb_controller.resource_probe import ProbeEngine
from mslb_persistence.database import Database

from scripts.check_deployment import check


def test_control_auth_host_origin_schema_and_raw_routes() -> None:
    async def run() -> None:
        engine = Engine(
            transport=httpx.MockTransport(
                handler=lambda _request: httpx.Response(
                    status_code=HTTPStatus.INTERNAL_SERVER_ERROR.value
                )
            )
        )
        database = Database(url="postgresql://unused/unused")
        app = create_app(database=database, token="a" * 64, engine=engine)
        try:
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://docker-proxy:8081"
                ) as client,
            ):
                assert (await client.get(url="/health")).status_code == HTTPStatus.OK.value
                assert (
                    await client.post(url="/control/enroll", json={})
                ).status_code == HTTPStatus.UNAUTHORIZED.value
                client.headers["Authorization"] = "Bearer " + "a" * 64
                for path in (
                    "/containers/json",
                    "/v1.53/containers/create",
                    "/docs",
                    "/openapi.json",
                ):
                    assert (
                        await client.post(url=path, json={})
                    ).status_code == HTTPStatus.NOT_FOUND.value
                assert (
                    await client.post(url="/control/enroll?token=hidden", json={})
                ).status_code == HTTPStatus.FORBIDDEN.value
                assert (
                    await client.post(
                        url="/control/enroll", json={}, headers={"Origin": "http://web"}
                    )
                ).status_code == HTTPStatus.FORBIDDEN.value
                assert (
                    await client.post(url="/control/enroll", json={}, headers={"Host": "api:8000"})
                ).status_code == HTTPStatus.BAD_REQUEST.value
                for payload, status in (
                    ({"Image": "anything"}, HTTPStatus.UNPROCESSABLE_CONTENT.value),
                    ({"x": "x" * 4096}, HTTPStatus.CONTENT_TOO_LARGE.value),
                ):
                    response = await client.post(url="/control/enroll", json=payload)
                    assert response.status_code == status and "anything" not in response.text
                response = await client.post(
                    url="/control/enroll", content=b"{}", headers={"Content-Type": "text/plain"}
                )
                assert response.status_code == HTTPStatus.UNSUPPORTED_MEDIA_TYPE.value
        finally:
            await engine.close()
            await database.close()

    asyncio.run(main=run())


def test_effective_configuration_protects_socket_and_credential() -> None:
    socket = {
        "type": "bind",
        "source": "/var/run/docker.sock",
        "target": "/var/run/docker.sock",
        "read_only": True,
    }
    secret = {
        "type": "volume",
        "source": "controller-auth",
        "target": "/run/controller-auth",
        "read_only": True,
    }
    check(
        payload=json.dumps(
            obj={"services": {"docker-proxy": {"volumes": [socket, secret]}}}
        ).encode()
    )
    for name, mount in (
        ("api", socket),
        ("worker", secret),
        ("docker-proxy", socket | {"read_only": False}),
        ("docker-proxy", socket | {"source": "/"}),
        ("container-controller", secret | {"read_only": False}),
    ):
        with pytest.raises(expected_exception=ValueError):
            check(payload=json.dumps(obj={"services": {name: {"volumes": [mount]}}}).encode())


def test_disconnected_caller_keeps_nonqueuing_action_slot_until_receipt_settles() -> None:
    async def run() -> None:
        entered, finish = asyncio.Event(), asyncio.Event()

        async def slow(_request: httpx.Request) -> httpx.Response:
            entered.set()
            await finish.wait()
            raise httpx.ConnectError(message="private daemon details")

        engine = Engine(transport=httpx.MockTransport(handler=slow))
        database = Database(url="postgresql://unused/unused")
        app = create_app(database=database, token="a" * 64, engine=engine)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://docker-proxy:8081",
                headers={"Authorization": "Bearer " + "a" * 64},
            ) as client,
        ):
            request = asyncio.create_task(coro=client.post(url="/control/enroll", json={}))
            await asyncio.wait_for(fut=entered.wait(), timeout=2)
            request.cancel()
            with pytest.raises(expected_exception=asyncio.CancelledError):
                await request
            try:
                response = await client.post(url="/control/enroll", json={})
                assert (
                    response.status_code == HTTPStatus.TOO_MANY_REQUESTS.value
                    and response.json() == {"error": "CONTROL_BUSY"}
                )
            finally:
                finish.set()

    asyncio.run(main=run())


def test_guard_credential_can_only_reach_fixed_readonly_measurements() -> None:
    async def run() -> None:
        calls: list[tuple[str, str]] = []

        def read(request: httpx.Request) -> httpx.Response:
            calls.append((request.method, request.url.path))
            return httpx.Response(status_code=HTTPStatus.INTERNAL_SERVER_ERROR.value)

        inspector = ProbeEngine(transport=httpx.MockTransport(handler=read))
        database = Database(url="postgresql://unused/unused")
        app = create_app(
            database=database, token="a" * 64, guard_token="b" * 64, probe_engine=inspector
        )
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://docker-proxy:8081"
            ) as client,
        ):
            guard = {"Authorization": "Bearer " + "b" * 64}
            controller = {"Authorization": "Bearer " + "a" * 64}
            for path in (
                "/control/enroll",
                "/containers/create",
                "/control/resources/anything/create",
            ):
                response = await client.post(url=path, headers=guard, json={})
                assert response.status_code == HTTPStatus.UNAUTHORIZED.value
            assert (
                await client.get(url="/guard/probe", headers=controller)
            ).status_code == HTTPStatus.UNAUTHORIZED.value
            assert calls == []
            response = await client.get(url="/guard/probe", headers=guard)
            assert response.status_code == HTTPStatus.BAD_GATEWAY.value and calls == [
                ("GET", "/v1.53/info")
            ]

    asyncio.run(main=run())


def test_deployment_isolates_guard_credential_from_api_worker_and_controller() -> None:
    mount = {
        "type": "volume",
        "source": "guard-auth",
        "target": "/run/guard-auth",
        "read_only": True,
    }
    check(payload=json.dumps(obj={"services": {"resource-guard": {"volumes": [mount]}}}).encode())
    for name in ("api", "worker", "container-controller"):
        with pytest.raises(expected_exception=ValueError):
            check(payload=json.dumps(obj={"services": {name: {"volumes": [mount]}}}).encode())

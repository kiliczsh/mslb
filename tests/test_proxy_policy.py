"""Credential, HTTP admission and effective deployment boundary without Docker access."""

import asyncio
import json

import httpx
import pytest
from mslb_controller.engine import Engine
from mslb_controller.proxy import create_app
from mslb_persistence.database import Database

from scripts.check_deployment import check


def test_control_auth_host_origin_schema_and_raw_routes() -> None:
    async def run() -> None:
        engine = Engine(httpx.MockTransport(lambda _request: httpx.Response(500)))
        database = Database("postgresql://unused/unused")
        app = create_app(database, "a" * 64, engine)
        try:
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app), base_url="http://docker-proxy:8081"
                ) as client,
            ):
                assert (await client.get("/health")).status_code == 200
                assert (await client.post("/control/enroll", json={})).status_code == 401
                client.headers["Authorization"] = "Bearer " + "a" * 64
                for path in (
                    "/containers/json",
                    "/v1.53/containers/create",
                    "/docs",
                    "/openapi.json",
                ):
                    assert (await client.post(path, json={})).status_code == 404
                assert (
                    await client.post("/control/enroll?token=hidden", json={})
                ).status_code == 403
                assert (
                    await client.post("/control/enroll", json={}, headers={"Origin": "http://web"})
                ).status_code == 403
                assert (
                    await client.post("/control/enroll", json={}, headers={"Host": "api:8000"})
                ).status_code == 400
                for payload, status in (({"Image": "anything"}, 422), ({"x": "x" * 4096}, 413)):
                    response = await client.post("/control/enroll", json=payload)
                    assert response.status_code == status and "anything" not in response.text
                response = await client.post(
                    "/control/enroll", content=b"{}", headers={"Content-Type": "text/plain"}
                )
                assert response.status_code == 415
        finally:
            await engine.close()
            await database.close()

    asyncio.run(run())


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
    check(json.dumps({"services": {"docker-proxy": {"volumes": [socket, secret]}}}).encode())
    for name, mount in (
        ("api", socket),
        ("worker", secret),
        ("docker-proxy", socket | {"read_only": False}),
        ("docker-proxy", socket | {"source": "/"}),
        ("container-controller", secret | {"read_only": False}),
    ):
        with pytest.raises(ValueError):
            check(json.dumps({"services": {name: {"volumes": [mount]}}}).encode())


def test_disconnected_caller_keeps_nonqueuing_action_slot_until_receipt_settles() -> None:
    async def run() -> None:
        entered, finish = asyncio.Event(), asyncio.Event()

        async def slow(_request: httpx.Request) -> httpx.Response:
            entered.set()
            await finish.wait()
            raise httpx.ConnectError("private daemon details")

        engine = Engine(httpx.MockTransport(slow))
        database = Database("postgresql://unused/unused")
        app = create_app(database, "a" * 64, engine)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app),
                base_url="http://docker-proxy:8081",
                headers={"Authorization": "Bearer " + "a" * 64},
            ) as client,
        ):
            request = asyncio.create_task(client.post("/control/enroll", json={}))
            await asyncio.wait_for(entered.wait(), 2)
            request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request
            try:
                response = await client.post("/control/enroll", json={})
                assert response.status_code == 429 and response.json() == {"error": "CONTROL_BUSY"}
            finally:
                finish.set()

    asyncio.run(run())

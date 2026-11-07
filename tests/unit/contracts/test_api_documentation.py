"""Offline reference assets never bypass access checks for the API schema."""

import asyncio
from http import HTTPStatus

import httpx
import pytest
from fastapi import FastAPI
from mslb_api.docs_ui import ASSETS, DOCS_ROOT, register_documentation
from mslb_api.guard import Guard
from mslb_api.settings import Settings


@pytest.mark.parametrize(argnames="authenticated", argvalues=[False, True])
def test_reference_assets_are_local_and_schema_respects_authentication(authenticated: bool) -> None:
    async def scenario() -> None:
        token = "synthetic-documentation-credential-" * 2
        app = FastAPI(openapi_url="/api/v1/openapi.json", docs_url=None, redoc_url=None)
        app.add_middleware(
            middleware_class=Guard,
            settings=Settings(
                auth_enabled=authenticated, token=token, allowed_hosts=("testserver",)
            ),
        )
        register_documentation(app=app)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            for path in ASSETS:
                response = await client.get(url=path)
                assert response.status_code == HTTPStatus.OK.value
                assert response.headers["x-content-type-options"] == "nosniff"
                assert response.headers["cache-control"] == "no-store"
                assert "script-src 'self'" in response.headers["content-security-policy"]
                assert "unsafe-eval" not in response.headers["content-security-policy"]
                assert token not in response.text
            shell = await client.get(url=DOCS_ROOT)
            assert 'type="module" src="/api/docs/client.js"' in shell.text
            script = await client.get(url=DOCS_ROOT + "/client.js")
            assert "disableTelemetry: true" in script.text
            assert "localStorage" not in script.text and "sessionStorage" not in script.text
            schema = await client.get(url="/api/v1/openapi.json")
            assert schema.status_code == (
                HTTPStatus.UNAUTHORIZED.value if authenticated else HTTPStatus.OK.value
            )
            authorized = await client.get(
                url="/api/v1/openapi.json", headers={"Authorization": "Bearer " + token}
            )
            assert authorized.status_code == HTTPStatus.OK.value
            assert "openapi" in authorized.json()

    asyncio.run(main=scenario())


@pytest.mark.parametrize(
    argnames="headers,query,status",
    argvalues=[
        ({"Host": "unexpected"}, "", HTTPStatus.FORBIDDEN),
        ({"Origin": "https://unexpected.example"}, "", HTTPStatus.FORBIDDEN),
        ({}, "?token=synthetic-url-secret", HTTPStatus.UNAUTHORIZED),
        ({}, "?access_token=synthetic-url-secret", HTTPStatus.UNAUTHORIZED),
    ],
)
def test_reference_shell_keeps_host_origin_and_credential_location_checks(
    headers: dict[str, str], query: str, status: HTTPStatus
) -> None:
    async def scenario() -> None:
        app = FastAPI()
        app.add_middleware(middleware_class=Guard, settings=Settings(allowed_hosts=("testserver",)))
        register_documentation(app=app)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            response = await client.get(url=DOCS_ROOT + query, headers=headers)
            assert response.status_code == status.value
            assert "synthetic-url-secret" not in response.text

    asyncio.run(main=scenario())

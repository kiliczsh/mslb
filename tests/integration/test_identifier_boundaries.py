"""Rejected IDs never reach PostgreSQL; accepted encoded IDs retain full CRUD access."""

import asyncio
from http import HTTPStatus
from pathlib import Path
from urllib.parse import quote

import httpx
from mslb_api.app import create_app
from mslb_api.settings import Settings
from mslb_application.files import ManagedFiles, StorageLimits

from tests.integration.helpers import scratch


def test_identifier_errors_are_field_specific_and_valid_encoded_ids_remain_manageable(
    tmp_path: Path,
) -> None:
    async def run() -> None:
        async with scratch() as database:
            limits = StorageLimits(min_free_bytes=0)
            inputs = ManagedFiles(root=tmp_path / "inputs", limits=limits)
            outputs = ManagedFiles(root=tmp_path / "outputs", limits=limits)
            app = create_app(
                settings=Settings(allowed_hosts=("testserver",)),
                database=database,
                inputs=inputs,
                outputs=outputs,
                signing_key=b"synthetic-identifier-boundary-key-" * 2,
            )
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://testserver/api/v1/"
                ) as client,
            ):
                before = (await client.get(url="servers")).json()
                for identifier in (".", "..", " . ", " .. ", "a\x00b"):
                    response = await client.post(
                        url="servers",
                        json={
                            "id": identifier,
                            "cpu_units_per_tick": 1,
                            "mem_mb": 1,
                            "rate_limit_per_sec": 1,
                        },
                    )
                    assert response.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
                    error = response.json()["error"]
                    assert error["code"] == "VALIDATION_ERROR"
                    assert any(detail["field"] == "body.id" for detail in error["details"])
                assert (await client.get(url="servers")).json() == before
                upload = await client.post(
                    url="datasets",
                    files={
                        "file": (
                            "nul.csv",
                            b"t,request_id,work_units,mem_mb\n0,a\x00b,1,1\n",
                            "text/csv",
                        )
                    },
                )
                assert upload.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
                assert upload.json()["error"]["code"] == "VALIDATION_ERROR"
                assert "request_id" in upload.json()["error"]["details"][0]["field"]
                assert (await client.get(url="datasets")).json()["items"] == []
                for identifier in (
                    "...",
                    "a.b",
                    "é/cluster",
                    "a/../b",
                    "%2e",
                    "server?part#key",
                ):
                    created = await client.post(
                        url="servers",
                        json={
                            "id": identifier,
                            "cpu_units_per_tick": 1,
                            "mem_mb": 1,
                            "rate_limit_per_sec": 1,
                        },
                    )
                    assert created.status_code == HTTPStatus.CREATED.value
                    path = "servers/" + quote(string=identifier, safe="")
                    assert (await client.get(url=path)).json()["id"] == identifier
                    changed = await client.patch(
                        url=path, json={"mem_mb": 2}, headers={"If-Match": '"1"'}
                    )
                    assert (
                        changed.status_code == HTTPStatus.OK.value and changed.json()["mem_mb"] == 2
                    )
                    assert (
                        await client.delete(url=path, headers={"If-Match": '"2"'})
                    ).status_code == HTTPStatus.NO_CONTENT.value

    asyncio.run(main=run())

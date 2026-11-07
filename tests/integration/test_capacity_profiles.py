"""An isolated installation owns stress capacity across real HTTP and durable work."""

import asyncio
from http import HTTPStatus
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from mslb_contracts.capacity_profiles import CapacityProfile
from mslb_persistence.catalog_models import Dataset
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

from tests.integration.mcp_http import http_workspace, worker


@pytest.mark.parametrize(argnames="profile", argvalues=["standard", "stress"])
def test_installation_profile_controls_upload_run_reads_and_database_constraints(
    *, tmp_path: Path, profile: CapacityProfile
) -> None:
    async def scenario() -> None:
        assert profile in {"standard", "stress"}
        async with (
            http_workspace(root=tmp_path, capacity_profile=profile) as workspace,
            httpx.AsyncClient(
                base_url=workspace.url.removesuffix("mcp") + "api/v1/", timeout=60
            ) as client,
        ):
            source = (
                "t,request_id,work_units,mem_mb\n"
                + "".join(f"0,r{index:05d},1,1\n" for index in range(10_001))
            ).encode()
            response = await client.post(
                url="datasets", files={"file": ("large.csv", source, "text/csv")}
            )
            if profile == "standard":
                assert response.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
                assert (await client.get(url="datasets")).json()["items"] == []
                return
            assert response.status_code == HTTPStatus.CREATED.value, response.text
            dataset_id = response.json()["id"]
            denied = await client.post(
                url="simulations",
                json={"dataset_id": dataset_id, "max_requests": 50_000},
            )
            assert denied.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
            server = await client.get(url="servers/s1")
            changed = await client.patch(
                url="servers/s1",
                headers={"If-Match": server.headers["etag"]},
                json={
                    "cpu_units_per_tick": 100_000,
                    "mem_mb": 100_000,
                    "rate_limit_per_sec": 100_000,
                },
            )
            assert changed.status_code == HTTPStatus.OK.value
            submitted = await client.post(
                url="simulations", json={"dataset_id": dataset_id, "mode": "concurrent"}
            )
            assert submitted.status_code == HTTPStatus.ACCEPTED.value, submitted.text
            run_id = submitted.json()["run_id"]
            assert await worker(workspace=workspace).run_once()
            detail = await client.get(url=f"simulations/{run_id}")
            assert detail.json()["status"] == "completed", detail.text
            assert detail.json()["configuration"]["max_requests"] == 50_000
            assert detail.json()["configuration"]["capacity_profile"] == "stress"
            metrics = await client.get(url=f"simulations/{run_id}/metrics")
            assert metrics.json()["summary"]["finished"] == 10_001
            artifact = await client.get(url=f"simulations/{run_id}/artifact")
            assert len(artifact.content.splitlines()) == 30_003
            cursor = None
            last_sequence = 0
            while True:
                parameters = {"limit": "500", "start_tick": "1", "end_tick": "1"}
                if cursor is not None:
                    parameters["cursor"] = cursor
                page = await client.get(url=f"simulations/{run_id}/events", params=parameters)
                assert page.status_code == HTTPStatus.OK.value, page.text
                last_sequence = page.json()["items"][-1]["sequence"]
                cursor = page.json()["next_cursor"]
                if cursor is None:
                    break
            assert last_sequence == 30_003
            playback = await client.get(
                url=f"simulations/{run_id}/playback",
                params={
                    "tick": 0,
                    "request_kind": "active",
                    "server_id": "s1",
                    "request_offset": 10_000,
                },
            )
            assert playback.status_code == HTTPStatus.OK.value, playback.text
            assert playback.json()["request_count"] == 10_001
            assert len(playback.json()["request_ids"]) == 1
            with pytest.raises(expected_exception=IntegrityError):
                async with workspace.database.sessions.begin() as session:
                    await session.execute(
                        statement=update(table=Dataset)
                        .where(Dataset.id == UUID(hex=dataset_id))
                        .values(capacity_profile="standard")
                    )

    asyncio.run(main=scenario())

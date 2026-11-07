"""Preview, bulk replacement and filtered history cross real REST/database boundaries."""

import asyncio
from http import HTTPStatus
from pathlib import Path

import httpx
import pytest
from mslb_contracts.queries import DatasetView

from tests.integration.http_helpers import workspace_client


async def upload_dataset(*, client: httpx.AsyncClient) -> str:
    """Upload generated in-memory input without changing supplied fixture files."""
    source = "request_id,t,work_units,mem_mb\n" + "".join(
        f"r{index:02},{index},2,1\n" for index in range(30)
    )
    response = await client.post(
        url="datasets", files={"file": ("workspace.csv", source.encode(), "text/csv")}
    )
    assert response.status_code == HTTPStatus.CREATED.value, response.text
    return str(object=DatasetView.model_validate_json(json_data=response.content).id)


@pytest.mark.parametrize(argnames="authenticated", argvalues=[False, True])
def test_dataset_preview_is_bounded_and_matches_verified_upload(
    tmp_path: Path, authenticated: bool
) -> None:
    async def scenario() -> None:
        async with workspace_client(directory=tmp_path, authenticated=authenticated) as client:
            identifier = await upload_dataset(client=client)
            response = await client.get(url=f"datasets/{identifier}/preview")
            assert response.status_code == HTTPStatus.OK.value
            preview = response.json()
            assert preview["dataset_id"] == identifier and preview["total_rows"] == 30
            assert preview["rows"] == [
                {"request_id": f"r{index:02}", "t": index, "work_units": 2, "mem_mb": 1}
                for index in range(20)
            ]
            assert "storage" not in preview and "path" not in preview
            assert (
                await client.get(url="datasets/00000000-0000-0000-0000-000000000001/preview")
            ).status_code == HTTPStatus.NOT_FOUND.value

    asyncio.run(main=scenario())


@pytest.mark.parametrize(argnames="authenticated", argvalues=[False, True])
def test_bulk_cluster_replacement_is_atomic_and_rejects_a_stale_review(
    tmp_path: Path, authenticated: bool
) -> None:
    async def scenario() -> None:
        async with workspace_client(directory=tmp_path, authenticated=authenticated) as client:
            original = (await client.get(url="servers")).json()["items"]
            replacement = [
                {"id": name, "cpu_units_per_tick": 4, "mem_mb": 64, "rate_limit_per_sec": 2}
                for name in ("worker-a", "worker-b", "worker-c")
            ]
            response = await client.put(
                url="servers", json={"expected": original, "servers": replacement}
            )
            assert response.status_code == HTTPStatus.OK.value, response.text
            saved = response.json()["items"]
            assert [
                {key: value for key, value in row.items() if key != "version"} for row in saved
            ] == replacement
            stale = await client.put(
                url="servers", json={"expected": original, "servers": replacement}
            )
            assert stale.status_code == HTTPStatus.CONFLICT.value
            assert (await client.get(url="servers")).json()["items"] == saved
            invalid = await client.put(
                url="servers", json={"expected": saved, "servers": replacement[:2]}
            )
            assert invalid.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
            assert (await client.get(url="servers")).json()["items"] == saved

    asyncio.run(main=scenario())


@pytest.mark.parametrize(argnames="authenticated", argvalues=[False, True])
def test_history_pages_are_filter_bound_and_cover_each_run_once(
    tmp_path: Path, authenticated: bool
) -> None:
    async def scenario() -> None:
        async with workspace_client(directory=tmp_path, authenticated=authenticated) as client:
            identifier = await upload_dataset(client=client)
            run_ids: list[str] = []
            for strategy in ("first_fit", "round_robin"):
                queued = await client.post(
                    url="simulations", json={"dataset_id": identifier, "strategy": strategy}
                )
                assert queued.status_code == HTTPStatus.ACCEPTED.value, queued.text
                run_ids.append(queued.json()["run_id"])
            filters = {"limit": 1, "status": "queued", "search": "workspace.csv"}
            first = await client.get(url="history/logical", params=filters)
            assert first.status_code == HTTPStatus.OK.value
            cursor = first.json()["next_cursor"]
            assert cursor is not None
            second = await client.get(url="history/logical", params=filters | {"cursor": cursor})
            assert second.status_code == HTTPStatus.OK.value
            assert [first.json()["items"][0]["id"], second.json()["items"][0]["id"]] == list(
                reversed(run_ids)
            )
            unrelated = await client.get(
                url="history/logical", params=filters | {"cursor": cursor, "status": "completed"}
            )
            assert unrelated.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
            filtered = await client.get(url="history/logical", params={"strategy": "first_fit"})
            assert [row["id"] for row in filtered.json()["items"]] == run_ids[:1]
            physical = await client.get(url="history/docker")
            assert physical.status_code == HTTPStatus.OK.value and physical.json()["items"] == []

    asyncio.run(main=scenario())

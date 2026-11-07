import asyncio
import io
import zipfile
from http import HTTPStatus
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from mslb_api.app import create_app
from mslb_api.settings import Settings
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_application.worker import Worker
from mslb_contracts.runs import RunSnapshot
from mslb_persistence.durability import Durability

from tests.integration.helpers import scratch


@pytest.mark.parametrize(argnames="authenticated", argvalues=[False, True])
def test_rest_crud_upload_durable_execution_downloads_and_sse(
    tmp_path: Path, authenticated: bool
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            inputs = await asyncio.to_thread(
                ManagedFiles, root=tmp_path / "inputs", limits=StorageLimits(min_free_bytes=0)
            )
            outputs = await asyncio.to_thread(
                ManagedFiles, root=tmp_path / "outputs", limits=StorageLimits(min_free_bytes=0)
            )
            token = "synthetic-http-test-secret-" * 2
            policy = Settings(
                auth_enabled=authenticated, token=token, allowed_hosts=("testserver",)
            )
            app = create_app(
                settings=policy,
                database=database,
                inputs=inputs,
                outputs=outputs,
                signing_key=b"synthetic-cursor-key-" * 2,
            )
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://testserver/api/v1/",
                    headers={"Authorization": "Bearer " + token} if authenticated else {},
                ) as client,
            ):
                health = await client.get(url="health/ready")
                assert (
                    health.status_code == HTTPStatus.SERVICE_UNAVAILABLE.value
                    and health.json()["api_ready"]
                )
                assert not health.json()["complete_product"]
                get = await client.get(url="servers/s1")
                assert get.status_code == HTTPStatus.OK.value and get.headers["etag"] == '"1"'
                missing = await client.patch(url="servers/s1", json={"mem_mb": 1000})
                assert missing.status_code == HTTPStatus.CONFLICT.value
                changed = await client.patch(
                    url="servers/s1", json={"mem_mb": 1000}, headers={"If-Match": '"1"'}
                )
                assert (
                    changed.status_code == HTTPStatus.OK.value and changed.headers["etag"] == '"2"'
                )
                stale = await client.delete(url="servers/s1", headers={"If-Match": '"1"'})
                assert stale.status_code == HTTPStatus.CONFLICT.value
                invalid = await client.patch(
                    url="servers/s1", json={"id": "renamed"}, headers={"If-Match": '"2"'}
                )
                assert invalid.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
                assert "input" not in invalid.json()["error"]["details"][0]
                create = await client.post(
                    url="servers",
                    json={
                        "id": " s3 ",
                        "cpu_units_per_tick": 1,
                        "mem_mb": 1,
                        "rate_limit_per_sec": 1,
                    },
                )
                assert (
                    create.status_code == HTTPStatus.CREATED.value and create.json()["id"] == "s3"
                )
                duplicate = await client.post(url="servers", json=create.json() | {"version": 1})
                assert duplicate.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
                source = await asyncio.to_thread(Path("/fixtures/core/requests.csv").read_bytes)
                upload = await client.post(
                    url="datasets", files={"file": ("requests.csv", source, "text/csv")}
                )
                assert upload.status_code == HTTPStatus.CREATED.value, upload.text
                dataset = upload.json()["id"]
                body = {"dataset_id": dataset, "strategy": "round_robin"}
                run = await client.post(
                    url="simulations", json=body, headers={"Idempotency-Key": "repeat-one"}
                )
                assert run.status_code == HTTPStatus.ACCEPTED.value, run.text
                run_id = run.json()["run_id"]
                detail = await client.get(url=f"simulations/{run_id}")
                assert detail.json()["configuration"]["mode"] == "sequential"
                assert detail.json()["mode"] == "sequential"
                repeated = await client.post(
                    url="simulations", json=body, headers={"Idempotency-Key": "repeat-one"}
                )
                assert repeated.json()["run_id"] == run_id
                conflict = await client.post(
                    url="simulations",
                    json=body | {"strategy": "first_fit"},
                    headers={"Idempotency-Key": "repeat-one"},
                )
                assert conflict.status_code == HTTPStatus.CONFLICT.value
                assert (
                    await client.get(url=f"simulations/{run_id}/artifact")
                ).status_code == HTTPStatus.CONFLICT.value
                assert (
                    await client.delete(url="servers/s1", headers={"If-Match": '"2"'})
                ).status_code == HTTPStatus.NO_CONTENT.value
                assert await Worker(database=database, inputs=inputs, outputs=outputs).run_once()
                detail = await client.get(url=f"simulations/{run_id}")
                assert (
                    detail.status_code == HTTPStatus.OK.value
                    and detail.json()["status"] == "completed"
                ), detail.text
                assert detail.json()["validation"]["independent_replay"] == "passed"
                download = await client.get(url=f"simulations/{run_id}/artifact")
                assert download.status_code == HTTPStatus.OK.value and download.content.endswith(
                    b"\n"
                )
                assert int(download.headers["content-length"]) == len(download.content)
                archive = await client.get(url=f"simulations/{run_id}/inputs")
                assert archive.status_code == HTTPStatus.OK.value
                with zipfile.ZipFile(file=io.BytesIO(initial_bytes=archive.content)) as bundle:
                    assert set(bundle.namelist()) == {
                        "servers.json",
                        "requests.csv",
                        "snapshot.json",
                        "manifest.json",
                    }
                    snapshot = RunSnapshot.model_validate_json(
                        json_data=bundle.read(name="snapshot.json")
                    )
                    assert "s1" in [server.id for server in snapshot.servers]
                metrics = await client.get(url=f"simulations/{run_id}/metrics")
                assert (
                    metrics.status_code == HTTPStatus.OK.value
                    and metrics.json()["summary"]["finished"] == 4
                )
                chart = await client.get(
                    url=f"simulations/{run_id}/metrics",
                    params={"metric": "cpu_utilization", "end_tick": 10, "max_points": 1},
                )
                assert chart.status_code == HTTPStatus.OK.value and len(chart.json()["points"]) == 1
                events = await client.get(url=f"simulations/{run_id}/events", params={"limit": 1})
                assert (
                    events.status_code == HTTPStatus.OK.value
                    and events.json()["items"][0]["sequence"] == 1
                )
                stream = await client.get(
                    url=f"simulations/{run_id}/stream", headers={"Last-Event-ID": "5"}
                )
                assert stream.status_code == HTTPStatus.OK.value
                ids = [
                    int(line[4:]) for line in stream.text.splitlines() if line.startswith("id: ")
                ]
                assert ids == list(range(6, 13))
                assert "event: progress" in stream.text and "event: status" in stream.text
                assert (
                    await client.get(
                        url=f"simulations/{run_id}/stream", headers={"Last-Event-ID": "150001"}
                    )
                ).status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
                schema = await client.get(url="openapi.json")
                assert (
                    schema.status_code == HTTPStatus.OK.value
                    and "SimulationCreate" in schema.json()["components"]["schemas"]
                )
                queued = await client.post(url="simulations", json=body)
                cancel = await client.post(url=f"simulations/{queued.json()['run_id']}/cancel")
                assert (
                    cancel.status_code == HTTPStatus.OK.value
                    and cancel.json()["status"] == "cancelled"
                )
                assert (
                    await client.post(url=f"simulations/{queued.json()['run_id']}/cancel")
                ).status_code == HTTPStatus.CONFLICT.value
                for malformed in (
                    {"dataset_id": dataset, "max_simulation_ticks": True},
                    {"dataset_id": dataset, "mode": "docker"},
                ):
                    assert (
                        await client.post(url="simulations", json=malformed)
                    ).status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
                candidates = []
                for strategy in ("first_fit", "round_robin"):
                    accepted = await client.post(
                        url="simulations", json=body | {"strategy": strategy}
                    )
                    assert accepted.status_code == HTTPStatus.ACCEPTED.value
                    candidates.append(accepted.json()["run_id"])
                    assert await Worker(
                        database=database, inputs=inputs, outputs=outputs
                    ).run_once()
                comparison = await client.post(
                    url="simulations/compare", json={"run_ids": candidates}
                )
                assert comparison.status_code == HTTPStatus.OK.value, comparison.text
                assert comparison.json()["baseline_run_id"] == candidates[0]
                incompatible = await client.post(
                    url="simulations/compare", json={"run_ids": [run_id, candidates[0]]}
                )
                assert incompatible.status_code == HTTPStatus.CONFLICT.value
                pointer = await Durability(database=database).published(run_id=UUID(hex=run_id))
                path = outputs.path(key=pointer.key).parent / "run.jsonl"
                await asyncio.to_thread(path.write_bytes, data=b"corrupted")
                broken = await client.get(url=f"simulations/{run_id}/artifact")
                assert broken.status_code == HTTPStatus.INTERNAL_SERVER_ERROR.value
                assert "content-disposition" not in broken.headers
                assert broken.json()["error"]["code"] == "VALIDATION_FAILED"
                slash_id = await client.post(
                    url="servers",
                    json={
                        "id": "a/b",
                        "cpu_units_per_tick": 1,
                        "mem_mb": 1,
                        "rate_limit_per_sec": 1,
                    },
                )
                assert slash_id.status_code == HTTPStatus.CREATED.value
                assert (await client.get(url="servers/a%2Fb")).json()["id"] == "a/b"
                assert (
                    await client.delete(url="servers/a%2Fb", headers={"If-Match": '"1"'})
                ).status_code == HTTPStatus.NO_CONTENT.value

    asyncio.run(main=scenario())

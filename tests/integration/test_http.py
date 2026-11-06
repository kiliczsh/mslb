import asyncio
import io
import zipfile
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


@pytest.mark.parametrize("authenticated", [False, True])
def test_rest_crud_upload_durable_execution_downloads_and_sse(
    tmp_path: Path, authenticated: bool
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            inputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "inputs", StorageLimits(min_free_bytes=0)
            )
            outputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "outputs", StorageLimits(min_free_bytes=0)
            )
            token = "synthetic-http-test-secret-" * 2
            policy = Settings(
                auth_enabled=authenticated, token=token, allowed_hosts=("testserver",)
            )
            app = create_app(policy, database, inputs, outputs, b"synthetic-cursor-key-" * 2)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://testserver/api/v1/",
                    headers={"Authorization": "Bearer " + token} if authenticated else {},
                ) as client:
                    health = await client.get("health/ready")
                    assert health.status_code == 503 and health.json()["api_ready"]
                    assert not health.json()["complete_product"]
                    get = await client.get("servers/s1")
                    assert get.status_code == 200 and get.headers["etag"] == '"1"'
                    missing = await client.patch("servers/s1", json={"mem_mb": 1000})
                    assert missing.status_code == 409
                    changed = await client.patch(
                        "servers/s1", json={"mem_mb": 1000}, headers={"If-Match": '"1"'}
                    )
                    assert changed.status_code == 200 and changed.headers["etag"] == '"2"'
                    stale = await client.delete("servers/s1", headers={"If-Match": '"1"'})
                    assert stale.status_code == 409
                    invalid = await client.patch(
                        "servers/s1", json={"id": "renamed"}, headers={"If-Match": '"2"'}
                    )
                    assert invalid.status_code == 422
                    assert "input" not in invalid.json()["error"]["details"][0]
                    create = await client.post(
                        "servers",
                        json={
                            "id": " s3 ",
                            "cpu_units_per_tick": 1,
                            "mem_mb": 1,
                            "rate_limit_per_sec": 1,
                        },
                    )
                    assert create.status_code == 201 and create.json()["id"] == "s3"
                    duplicate = await client.post("servers", json=create.json() | {"version": 1})
                    assert duplicate.status_code == 422
                    source = await asyncio.to_thread(Path("/fixtures/core/requests.csv").read_bytes)
                    upload = await client.post(
                        "datasets", files={"file": ("requests.csv", source, "text/csv")}
                    )
                    assert upload.status_code == 201, upload.text
                    dataset = upload.json()["id"]
                    body = {"dataset_id": dataset, "strategy": "round_robin"}
                    run = await client.post(
                        "simulations", json=body, headers={"Idempotency-Key": "repeat-one"}
                    )
                    assert run.status_code == 202, run.text
                    run_id = run.json()["run_id"]
                    repeated = await client.post(
                        "simulations", json=body, headers={"Idempotency-Key": "repeat-one"}
                    )
                    assert repeated.json()["run_id"] == run_id
                    conflict = await client.post(
                        "simulations",
                        json=body | {"strategy": "first_fit"},
                        headers={"Idempotency-Key": "repeat-one"},
                    )
                    assert conflict.status_code == 409
                    assert (await client.get(f"simulations/{run_id}/artifact")).status_code == 409
                    assert (
                        await client.delete("servers/s1", headers={"If-Match": '"2"'})
                    ).status_code == 204
                    assert await Worker(database, inputs, outputs).run_once()
                    detail = await client.get(f"simulations/{run_id}")
                    assert detail.status_code == 200 and detail.json()["status"] == "completed", (
                        detail.text
                    )
                    assert detail.json()["validation"]["independent_replay"] == "passed"
                    download = await client.get(f"simulations/{run_id}/artifact")
                    assert download.status_code == 200 and download.content.endswith(b"\n")
                    assert int(download.headers["content-length"]) == len(download.content)
                    archive = await client.get(f"simulations/{run_id}/inputs")
                    assert archive.status_code == 200
                    with zipfile.ZipFile(io.BytesIO(archive.content)) as bundle:
                        assert set(bundle.namelist()) == {
                            "servers.json",
                            "requests.csv",
                            "snapshot.json",
                            "manifest.json",
                        }
                        snapshot = RunSnapshot.model_validate_json(bundle.read("snapshot.json"))
                        assert "s1" in [server.id for server in snapshot.servers]
                    metrics = await client.get(f"simulations/{run_id}/metrics")
                    assert metrics.status_code == 200 and metrics.json()["summary"]["finished"] == 4
                    chart = await client.get(
                        f"simulations/{run_id}/metrics",
                        params={"metric": "cpu_utilization", "end_tick": 10, "max_points": 1},
                    )
                    assert chart.status_code == 200 and len(chart.json()["points"]) == 1
                    events = await client.get(f"simulations/{run_id}/events", params={"limit": 1})
                    assert events.status_code == 200 and events.json()["items"][0]["sequence"] == 1
                    stream = await client.get(
                        f"simulations/{run_id}/stream", headers={"Last-Event-ID": "5"}
                    )
                    assert stream.status_code == 200
                    ids = [
                        int(line[4:])
                        for line in stream.text.splitlines()
                        if line.startswith("id: ")
                    ]
                    assert ids == list(range(6, 13))
                    assert "event: progress" in stream.text and "event: status" in stream.text
                    assert (
                        await client.get(
                            f"simulations/{run_id}/stream", headers={"Last-Event-ID": "30001"}
                        )
                    ).status_code == 422
                    schema = await client.get("openapi.json")
                    assert (
                        schema.status_code == 200
                        and "SimulationCreate" in schema.json()["components"]["schemas"]
                    )
                    queued = await client.post("simulations", json=body)
                    cancel = await client.post(f"simulations/{queued.json()['run_id']}/cancel")
                    assert cancel.status_code == 200 and cancel.json()["status"] == "cancelled"
                    assert (
                        await client.post(f"simulations/{queued.json()['run_id']}/cancel")
                    ).status_code == 409
                    for malformed in (
                        {"dataset_id": dataset, "max_simulation_ticks": True},
                        {"dataset_id": dataset, "mode": "docker"},
                    ):
                        assert (await client.post("simulations", json=malformed)).status_code == 422
                    candidates = []
                    for strategy in ("first_fit", "round_robin"):
                        accepted = await client.post(
                            "simulations", json=body | {"strategy": strategy}
                        )
                        assert accepted.status_code == 202
                        candidates.append(accepted.json()["run_id"])
                        assert await Worker(database, inputs, outputs).run_once()
                    comparison = await client.post(
                        "simulations/compare", json={"run_ids": candidates}
                    )
                    assert comparison.status_code == 200, comparison.text
                    assert comparison.json()["baseline_run_id"] == candidates[0]
                    incompatible = await client.post(
                        "simulations/compare", json={"run_ids": [run_id, candidates[0]]}
                    )
                    assert incompatible.status_code == 409
                    pointer = await Durability(database).published(UUID(run_id))
                    path = outputs.path(pointer.key).parent / "run.jsonl"
                    await asyncio.to_thread(path.write_bytes, b"corrupted")
                    broken = await client.get(f"simulations/{run_id}/artifact")
                    assert broken.status_code == 500
                    assert "content-disposition" not in broken.headers
                    assert broken.json()["error"]["code"] == "VALIDATION_FAILED"
                    slash_id = await client.post(
                        "servers",
                        json={
                            "id": "a/b",
                            "cpu_units_per_tick": 1,
                            "mem_mb": 1,
                            "rate_limit_per_sec": 1,
                        },
                    )
                    assert slash_id.status_code == 201
                    assert (await client.get("servers/a%2Fb")).json()["id"] == "a/b"
                    assert (
                        await client.delete("servers/a%2Fb", headers={"If-Match": '"1"'})
                    ).status_code == 204

    asyncio.run(scenario())

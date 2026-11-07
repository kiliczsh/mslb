"""Public mode discrimination, trusted enrollment, idempotency and isolated lifecycle reads."""

import asyncio
from http import HTTPStatus
from pathlib import Path

import httpx
import pytest
from mslb_api.app import create_app, get_mcp
from mslb_api.settings import Settings
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_contracts.docker_runs import DeploymentSpec, StaticBackend
from mslb_persistence.docker_catalog import DockerCatalog
from mslb_persistence.resource_evidence import ResourceEvidenceStore

from tests.integration.helpers import scratch
from tests.integration.test_resource_evidence import healthy_evidence


@pytest.mark.parametrize(argnames="authenticated", argvalues=(False, True))
def test_docker_http_requires_trusted_evidence_and_never_enters_logical_history(
    tmp_path: Path, authenticated: bool
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            limits = StorageLimits(min_free_bytes=0)
            inputs = ManagedFiles(root=tmp_path / "inputs", limits=limits)
            outputs = ManagedFiles(root=tmp_path / "outputs", limits=limits)
            token = "synthetic-docker-http-secret-" * 2
            app = create_app(
                settings=Settings(
                    auth_enabled=authenticated, token=token, allowed_hosts=("testserver",)
                ),
                database=database,
                inputs=inputs,
                outputs=outputs,
                signing_key=b"synthetic-docker-cursor" * 2,
            )
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://testserver/api/v1/",
                    headers={"Authorization": "Bearer " + token} if authenticated else {},
                ) as client,
            ):
                upload = await client.post(
                    url="datasets",
                    files={
                        "file": (
                            "http.csv",
                            b"t,request_id,work_units,mem_mb\n0,one,1,1\n",
                            "text/csv",
                        )
                    },
                )
                assert upload.status_code == HTTPStatus.CREATED.value
                body = {"dataset_id": upload.json()["id"], "mode": "docker", "duration_seconds": 3}
                rejected = await client.post(url="simulations", json=body)
                assert rejected.status_code == HTTPStatus.SERVICE_UNAVAILABLE.value
                assert rejected.json()["error"]["code"] == "RESOURCE_PRESSURE"
                implicit = await client.post(
                    url="simulations",
                    json={"dataset_id": body["dataset_id"], "initial_replicas": 3},
                )
                assert implicit.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
                # Synthetic evidence isolates HTTP/SQL; daemon/host safety is tested separately.
                probe, decision = healthy_evidence()
                ids = {
                    sample.service: sample.container_id
                    for sample in probe.services
                    if sample.container_id is not None
                }
                static = tuple(
                    StaticBackend(name=name, container_id=ids[name])
                    for name in ("backend-a", "backend-b")
                )
                await DockerCatalog(database=database).enroll(
                    definition=DeploymentSpec(
                        image_id="sha256:" + "a" * 64, network_id="b" * 64, static=static
                    )
                )
                store = ResourceEvidenceStore(database=database)
                publisher = await store.claim(owner="synthetic-http-guard")
                assert publisher is not None
                await store.publish(
                    lease=publisher,
                    expected_revision=publisher.revision,
                    probe=probe,
                    decision=decision,
                )
                submitted = await client.post(
                    url="simulations", json=body, headers={"Idempotency-Key": "docker-http"}
                )
                assert submitted.status_code == HTTPStatus.ACCEPTED.value, submitted.text
                run_id = submitted.json()["run_id"]
                async with get_mcp(app=app).services.submissions.admission.lock:
                    blocked = await client.post(
                        url="docker/simulations",
                        json=body,
                        headers={"Idempotency-Key": "new-while-busy"},
                    )
                    assert blocked.status_code == HTTPStatus.TOO_MANY_REQUESTS.value
                    logical_blocked = await client.post(
                        url="simulations", json={"dataset_id": body["dataset_id"]}
                    )
                    assert logical_blocked.status_code == HTTPStatus.TOO_MANY_REQUESTS.value
                    recovered = await client.post(
                        url="simulations", json=body, headers={"Idempotency-Key": "docker-http"}
                    )
                    assert recovered.json()["run_id"] == run_id
                assert submitted.headers["location"] == f"/api/v1/docker/simulations/{run_id}"
                repeated = await client.post(
                    url="docker/simulations", json=body, headers={"Idempotency-Key": "docker-http"}
                )
                assert repeated.json()["run_id"] == run_id
                conflict = await client.post(
                    url="docker/simulations",
                    json=body | {"initial_replicas": 1},
                    headers={"Idempotency-Key": "docker-http"},
                )
                assert conflict.status_code == HTTPStatus.CONFLICT.value
                assert (await client.get(url="simulations")).json()["items"] == []
                assert (await client.get(url="docker/simulations")).json()["items"][0][
                    "id"
                ] == run_id
                detail = await client.get(url=f"docker/simulations/{run_id}")
                assert detail.json()["run"]["configuration"]["mode"] == "docker"
                assert detail.json()["summary"] is None and detail.json()["artifacts"] == []
                assert "snapshot_key" not in detail.text and "network_id" not in detail.text
                assert (
                    await client.get(url=f"simulations/{run_id}")
                ).status_code == HTTPStatus.NOT_FOUND.value
                assert (
                    await client.get(url=f"docker/simulations/{run_id}/artifact")
                ).status_code == HTTPStatus.CONFLICT.value
                for extra in (
                    {"image": "untrusted"},
                    {"autoscaling": {"enabled": True}},
                    {"deployment_id": "untrusted"},
                ):
                    response = await client.post(url="simulations", json=body | extra)
                    assert response.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
                cancelled = await client.post(url=f"docker/simulations/{run_id}/cancel")
                assert cancelled.status_code == HTTPStatus.OK.value
                assert cancelled.json()["run"]["status"] == "cancelled"

    asyncio.run(main=scenario())

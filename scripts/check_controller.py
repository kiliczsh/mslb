"""Connected durable controller checks against the test proxy and real owned backends."""

import asyncio
import io
import tempfile
from pathlib import Path
from typing import override

import httpx
from mslb_application.docker_inputs import DockerSubmissions, load_docker_snapshot
from mslb_application.docker_outputs import verify_docker_outputs
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_artifacts import HttpRecord, LifecycleRecord
from mslb_contracts.docker_runs import DockerManifest, DockerSettings
from mslb_controller.control_client import ControlClient
from mslb_controller.controller import Controller
from mslb_persistence.catalog import Catalog
from mslb_persistence.database import Database
from mslb_persistence.docker_catalog import DockerCatalog
from mslb_persistence.jobs import Jobs
from mslb_persistence.models import DockerPublication, DockerResource
from sqlalchemy import select


class LoopbackControl(httpx.AsyncBaseTransport):
    """Redirect fixed semantic test RPC to the disposable-schema proxy in this container."""

    def __init__(self) -> None:
        self.transport = httpx.AsyncHTTPTransport(retries=0)

    @override
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        request.url = request.url.copy_with(host="127.0.0.1", port=8081)
        return await self.transport.handle_async_request(request=request)

    @override
    async def aclose(self) -> None:
        await self.transport.aclose()


async def check_controller(*, database: Database) -> None:
    """Execute all strategies, real dynamic membership and one complete scaling cycle."""
    client = ControlClient(transport=LoopbackControl())
    try:
        deployment = await client.enroll()
        with tempfile.TemporaryDirectory(prefix="mslb-controller-") as directory:
            root, limits = Path(directory), StorageLimits(min_free_bytes=0)
            inputs = ManagedFiles(root=root / "inputs", limits=limits)
            outputs = ManagedFiles(root=root / "outputs", limits=limits)
            catalog, docker = Catalog(database=database), DockerCatalog(database=database)
            submissions = DockerSubmissions(catalog=catalog, docker=docker, files=inputs)
            controller = Controller(
                database=database, inputs=inputs, outputs=outputs, client=client
            )
            configurations = [
                DockerSettings.model_validate(
                    obj={"strategy": strategy, "initial_replicas": count, "duration_seconds": 5}
                )
                for strategy, count in (
                    ("first_fit", 1),
                    ("round_robin", 4),
                    ("least_connections", 2),
                    ("most_available_memory", 3),
                )
            ]
            configurations.append(
                DockerSettings.model_validate(
                    obj={
                        "initial_replicas": 1,
                        "max_replicas": 3,
                        "starts_per_backend_second": 1,
                        "pending_timeout_seconds": 20,
                        "duration_seconds": 20,
                        "physical_scaling": {"enabled": True},
                    }
                )
            )
            for number, settings in enumerate(iterable=configurations):
                scaling = settings.physical_scaling.enabled
                csv = (
                    b"t,request_id,work_units,mem_mb\n"
                    + b"".join(
                        f"0,r{index:02},1,1\n".encode() for index in range(18 if scaling else 8)
                    )
                    + (b"16,tail,1,1\n" if scaling else b"")
                )
                registration = inputs.ingest(
                    source=io.BytesIO(initial_bytes=csv), filename="controller.csv"
                )
                dataset = await catalog.register_dataset(data=registration)
                run = await submissions.submit(
                    dataset_id=dataset.id,
                    deployment_id=deployment.id,
                    settings=settings,
                    key=f"controller-{number}",
                )
                if not await asyncio.wait_for(fut=controller.run_once(), timeout=60):
                    raise RuntimeError("Controller did not claim the registered run")
                job = await Jobs(database=database).get(run_id=run.id)
                if job.status != "completed" or job.attempts != 1:
                    raise RuntimeError(
                        f"Controller did not complete: {job.status}/{job.error_code}/"
                        f"{controller.last_error_code}"
                    )
                async with database.sessions() as session:
                    publication = await session.get(entity=DockerPublication, ident=run.id)
                    resources = tuple(
                        await session.scalars(
                            statement=select(DockerResource).where(DockerResource.run_id == run.id)
                        )
                    )
                    if publication is None or any(
                        resource.state not in {"removed", "released", "abandoned"}
                        for resource in resources
                    ):
                        raise RuntimeError("Controller publication or owned cleanup is incomplete")
                    manifest = DockerManifest.model_validate_json(
                        json_data=canonical_bytes(value=publication.manifest)
                    )
                snapshot = load_docker_snapshot(
                    files=inputs, key=run.snapshot_key, expected_digest=run.snapshot_digest
                )
                requests = inputs.load_requests(
                    digest=dataset.digest, size_bytes=dataset.size_bytes
                )
                runtime = verify_docker_outputs(
                    files=outputs, manifest=manifest, snapshot=snapshot, requests=requests
                )
                if runtime.traffic.completed != len(requests):
                    raise RuntimeError("Controller did not deliver every real request")
                if scaling and not any(resource.kind == "dynamic" for resource in resources):
                    raise RuntimeError("Sustained queue did not create a real dynamic backend")
                if scaling:
                    with outputs.path(key=manifest.artifacts["http_events.jsonl"].key).open(
                        mode="rb"
                    ) as stream:
                        events = [HttpRecord.model_validate_json(json_data=line) for line in stream]
                    tail = next(
                        event
                        for event in events
                        if event.request_id == "tail" and event.kind == "arrived"
                    )
                    with outputs.path(key=manifest.artifacts["container_lifecycle.jsonl"].key).open(
                        mode="rb"
                    ) as stream:
                        lifecycle = [
                            LifecycleRecord.model_validate_json(json_data=line) for line in stream
                        ]
                    if not any(
                        record.kind == "dynamic"
                        and record.state == "removed"
                        and record.observed_at < tail.observed_at
                        for record in lifecycle
                    ):
                        raise RuntimeError(
                            "Owned idle dynamic scale-in did not precede the later arrival"
                        )
                print(
                    f"Durable controller {settings.strategy}: {len(requests)} actual completions; "
                    f"scaling={scaling}"
                )
    finally:
        await client.close()

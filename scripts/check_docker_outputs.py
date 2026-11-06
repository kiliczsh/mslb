"""Actual proxy ownership, scheduled HTTP work, verified files and fenced publication."""

import asyncio
import io
import tempfile
from http import HTTPStatus
from pathlib import Path

import httpx
from mslb_application.docker_inputs import DockerSubmissions, load_docker_snapshot
from mslb_application.docker_outputs import DockerOutputs, verify_docker_outputs
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_contracts.docker_runs import DockerSettings, DockerSnapshot, StaticReservation
from mslb_controller.engine import Engine
from mslb_controller.lifecycle import Lifecycle
from mslb_controller.proxy import LeaseRef
from mslb_controller.routing import BackendHttp, BackendSpec
from mslb_controller.traffic import TrafficDriver
from mslb_persistence.catalog import Catalog
from mslb_persistence.database import Database
from mslb_persistence.docker_catalog import DockerCatalog
from mslb_persistence.docker_control import DockerControl
from mslb_persistence.docker_resources import DockerResources
from mslb_persistence.models import DockerPublication, PublishedArtifact
from sqlalchemy import func, select


def require(*, condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def private_files_clean(*, root: Path) -> bool:
    return not (root / "outputs" / "runs").exists() and list(root.rglob(pattern="*.spool")) == []


async def check_outputs(
    *, database: Database, client: httpx.AsyncClient, backend: BackendHttp, engine: Engine
) -> None:
    with tempfile.TemporaryDirectory(prefix="mslb-docker-publication-") as directory:
        root = Path(directory)
        # Explicit test-only small temporary filesystem; product free-space guards are unchanged.
        limits = StorageLimits(min_free_bytes=0)
        inputs = ManagedFiles(root=root / "inputs", limits=limits)
        outputs = ManagedFiles(root=root / "outputs", limits=limits)
        catalog, docker = Catalog(database=database), DockerCatalog(database=database)
        registration = inputs.ingest(
            source=io.BytesIO(
                initial_bytes=b"t,request_id,work_units,mem_mb\n0,first,1,1\n1,second,1,1\n"
            ),
            filename="real-http.csv",
        )
        dataset = await catalog.register_dataset(data=registration)
        deployment = await Lifecycle(database=database, engine=engine, http=backend).enroll()
        submission = DockerSubmissions(catalog=catalog, docker=docker, files=inputs)
        run = await submission.submit(
            dataset_id=dataset.id,
            deployment_id=deployment.id,
            settings=DockerSettings(duration_seconds=3),
            key="real-output",
        )
        snapshot: DockerSnapshot = load_docker_snapshot(
            files=inputs, key=run.snapshot_key, expected_digest=run.snapshot_digest
        )
        requests = inputs.load_requests(digest=dataset.digest, size_bytes=dataset.size_bytes)
        control, resources = DockerControl(database=database), DockerResources(database=database)
        lease = await control.claim(owner="artifact-check", lease_seconds=120)
        if lease is None:
            raise RuntimeError("Real output check could not claim Docker execution")
        reserved = tuple(
            [
                await resources.reserve_static(lease=lease, name=name)
                for name in ("backend-a", "backend-b")
            ]
        )
        specs: list[BackendSpec] = []
        for resource in reserved:
            if not isinstance(resource.definition, StaticReservation):
                raise RuntimeError("Real output static reservation differs")
            definition = resource.definition.backend
            specs.append(
                BackendSpec(
                    id=definition.name,
                    logical_admission_mem_mb=definition.logical_admission_mem_mb,
                    max_inflight=definition.max_inflight,
                )
            )
        async with DockerOutputs(
            files=outputs, run_id=run.id, fence=lease.fence, snapshot=snapshot, requests=requests
        ) as writer:
            for resource in reserved:
                await writer.observe(resource=resource)
            summary = await TrafficDriver(
                configuration=snapshot.configuration, specs=tuple(specs), http=backend, sink=writer
            ).execute(requests=requests)
            require(
                condition=summary.completed == summary.total == 2,
                message="Real output HTTP execution failed",
            )
            cleanup = await control.begin_cleanup(lease=lease, outcome="completed")
            reference = LeaseRef(
                run_id=cleanup.run_id, fence=cleanup.fence, owner=cleanup.owner, phase=cleanup.phase
            )
            for resource in reserved:
                response = await client.post(
                    url=f"/control/resources/{resource.id}/cleanup",
                    content=reference.model_dump_json(),
                )
                require(
                    condition=response.status_code == HTTPStatus.OK.value,
                    message="Real output static cleanup failed",
                )
                await writer.observe(
                    resource=await resources.get(lease=cleanup, resource_id=resource.id)
                )
            manifest = await writer.finalize(summary=summary)
            completed = await control.finish_cleanup(lease=cleanup, manifest=manifest)
        require(
            condition=completed.status == "completed" and completed.attempts == 1,
            message="Real Docker publication did not complete",
        )
        await asyncio.to_thread(
            verify_docker_outputs,
            files=outputs,
            manifest=manifest,
            snapshot=snapshot,
            requests=requests,
        )
        async with database.sessions() as session:
            published = await session.get(entity=DockerPublication, ident=run.id)
            require(
                condition=published is not None and published.digest == manifest.digest(),
                message="Real Docker artifact pointer differs",
            )
            require(
                condition=await session.scalar(
                    statement=select(func.count()).select_from(PublishedArtifact)
                )
                == 0,
                message="Docker output entered the logical artifact registry",
            )
        require(
            condition=await asyncio.to_thread(private_files_clean, root=root),
            message="Private Docker spool or logical output leaked",
        )
    print(
        "Actual scheduled HTTP, managed snapshots, verified files "
        "and fenced Docker publication passed"
    )

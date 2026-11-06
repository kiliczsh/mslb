"""Real project-scoped proxy/lifecycle and process-crash checks in a disposable schema."""

import asyncio
import os
import sys
from dataclasses import replace
from uuid import UUID

import httpx
from mslb_contracts.docker_runs import DockerSettings, DockerSnapshot
from mslb_contracts.traffic import WorkRequest
from mslb_controller.control_client import ControlClient
from mslb_controller.credentials import read_token
from mslb_controller.engine import Engine, EngineError
from mslb_controller.lifecycle import Lifecycle
from mslb_controller.proxy import LeaseRef
from mslb_controller.routing import BackendHttp, BackendSpec, Router
from mslb_persistence.database import Database
from mslb_persistence.docker_catalog import DockerCatalog
from mslb_persistence.docker_control import DockerControl
from mslb_persistence.docker_records import ControlLease
from mslb_persistence.docker_resources import DockerResources

from scripts.check_docker_outputs import check_outputs
from tests.integration.helpers import scratch
from tests.integration.test_docker_control import expire
from tests.integration.test_persistence import prepare


def require(value: bool, message: str) -> None:
    if not value:
        raise RuntimeError(message)


async def server(database: Database) -> asyncio.subprocess.Process:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "mslb_controller.proxy_main",
        env=os.environ
        | {
            "DATABASE_URL": os.environ["TEST_DATABASE_URL"],
            "CONTROL_DATABASE_SCHEMA": database.schema,
        },
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    async with httpx.AsyncClient(trust_env=False, timeout=1) as client:
        for _ in range(80):
            if process.returncode is not None:
                break
            try:
                if (await client.get("http://127.0.0.1:8081/health")).status_code == 200:
                    return process
            except httpx.HTTPError:
                pass
            await asyncio.sleep(0.1)
    await stop(process)
    raise RuntimeError("Test proxy did not become ready")


async def stop(process: asyncio.subprocess.Process, *, crash: bool = False) -> None:
    if process.returncode is None:
        if crash:
            process.kill()
        else:
            process.terminate()
    async with asyncio.timeout(55):
        output, _error = await process.communicate()
    require(len(output) < 32768, "Proxy log bound exceeded")
    require(
        not any(word in output.upper() for word in (b"WARNING", b"ERROR", b"TRACEBACK")),
        "Proxy emitted a diagnostic during lifecycle verification",
    )


async def rpc(
    client: httpx.AsyncClient, lease: ControlLease, identifier: UUID, action: str, status: int = 200
) -> None:
    reference = LeaseRef(
        run_id=lease.run_id, fence=lease.fence, owner=lease.owner, phase=lease.phase
    )
    response = await client.post(
        f"/control/resources/{identifier}/{action}", content=reference.model_dump_json()
    )
    require(
        response.status_code == status,
        f"Lifecycle {action} returned {response.status_code}: {response.text}",
    )


async def enqueue(database: Database) -> ControlLease:
    engine, http = Engine(), BackendHttp()
    try:
        deployment = await Lifecycle(database, engine, http).enroll()
    finally:
        await engine.close()
        await http.client.aclose()
    logical = await prepare(database)
    await DockerCatalog(database).enqueue(
        DockerSnapshot(
            dataset_id=logical.dataset.id,
            dataset_digest=logical.dataset.digest,
            request_count=logical.dataset.request_count,
            latest_arrival=logical.dataset.latest_arrival,
            deployment_id=deployment.id,
            deployment_digest=deployment.definition.digest(),
            configuration=DockerSettings(),
        )
    )
    lease = await DockerControl(database).claim("lifecycle-check", 120)
    if lease is None:
        raise RuntimeError("Test Docker claim failed")
    return lease


async def crash_create() -> None:
    """Abrupt exit after real daemon success, optionally after persisting its immutable ID."""
    schema, run_id, identifier, fence, phase = sys.argv[2:]
    database = Database(os.environ["TEST_DATABASE_URL"], schema)
    lease = ControlLease(
        UUID(run_id),
        int(fence),
        "lifecycle-check",
        LeaseRef(run_id=UUID(run_id), fence=int(fence), owner="lifecycle-check", phase="running")
        .lease()
        .expires_at,
        "running",
    )
    resources, engine = DockerResources(database), Engine()
    intent = await resources.authorize_create(lease, UUID(identifier))
    created = await engine.create(intent)
    require(not bool(created.Warnings), "Create emitted daemon diagnostics")
    if phase == "observed":
        http = BackendHttp()
        await Lifecycle(database, engine, http).observe(lease, UUID(identifier))
    os._exit(73)  # Deliberate process death: no Python finalizers/ID receipt in the creating case.


async def main() -> None:
    # The actual installed service accepts only its own scoped credential; no raw API route.
    scoped = ControlClient()
    try:
        deployment = await scoped.enroll()
        require(deployment.definition.network_id != "", "Installed proxy enrollment failed")
    finally:
        await scoped.close()
    async with scratch(retain_on_failure=True) as database:
        resources, control = DockerResources(database), DockerControl(database)
        process = await server(database)
        engine, backend = Engine(), BackendHttp()
        auth = {"Authorization": "Bearer " + read_token(), "Content-Type": "application/json"}
        try:
            async with httpx.AsyncClient(
                base_url="http://127.0.0.1:8081", headers=auth, trust_env=False, timeout=50
            ) as client:
                require(
                    (await client.post("/containers/create", json={})).status_code == 404,
                    "Raw API exposed",
                )
                require(
                    (
                        await client.post(
                            "/control/enroll", json={}, headers={"Authorization": "Bearer invalid"}
                        )
                    ).status_code
                    == 401,
                    "Bad token accepted",
                )
                lease = await enqueue(database)
                static = await resources.reserve_static(lease, "backend-a")
                record = await resources.prepare_dynamic(lease)
                owned_id: str | None = None
                try:
                    await rpc(
                        client, replace(lease, fence=lease.fence + 1), record.id, "create", 409
                    )
                    await rpc(client, lease, record.id, "create")
                    owned = await resources.get(lease, record.id)
                    owned_id = owned.container_id
                    require(
                        owned_id is not None and owned.state == "observed",
                        "ID not saved before start",
                    )
                    await rpc(client, lease, record.id, "create", 409)
                    await rpc(client, lease, record.id, "start")
                    spec = BackendSpec(record.name)
                    router = Router((spec,), "first_fit", backend)
                    for _ in range(40):
                        await router.refresh()
                        if router.states[spec.id].healthy:
                            break
                        await asyncio.sleep(0.1)
                    result = await backend.work(
                        router.reserve(
                            WorkRequest(request_id="lifecycle-real", work_units=1, mem_mb=1)
                        )
                    )
                    require(
                        result.backend_id == record.name and result.operations == 2048,
                        "Actual work was not executed",
                    )
                    await rpc(client, lease, static.id, "start", 409)
                finally:
                    cleanup = await control.begin_cleanup(lease, "failed", "CONTROLLER_FAILURE")
                    await rpc(client, cleanup, record.id, "cleanup")
                    await rpc(client, cleanup, static.id, "cleanup")
                    await control.finish_cleanup(cleanup)
                if owned_id is None:
                    raise RuntimeError("Container ID missing")
                try:
                    await engine.inspect(owned_id)
                except EngineError as error:
                    require(error.status == 404, "Removed container absence not verified")
                else:
                    raise RuntimeError("Owned container survived cleanup")
                # Before create, after daemon success, and after ID persistence: never replay work.
                for phase in ("prepared", "creating", "observed"):
                    lease = await enqueue(database)
                    record = await resources.prepare_dynamic(lease)
                    if phase != "prepared":
                        child = await asyncio.create_subprocess_exec(
                            sys.executable,
                            "-m",
                            "scripts.check_lifecycle",
                            "crash-create",
                            database.schema,
                            str(lease.run_id),
                            str(record.id),
                            str(lease.fence),
                            phase,
                        )
                        require(
                            await child.wait() == 73,
                            "Fault process did not reach intended crash window",
                        )
                    await stop(process, crash=True)
                    await expire(database)
                    recovered = await control.claim("recovery", 120)
                    require(
                        recovered is not None and recovered.phase == "cleanup",
                        "Interrupted work was replayed",
                    )
                    if recovered is None:
                        raise RuntimeError("Cleanup claim missing")
                    process = await server(database)
                    await rpc(client, lease, record.id, "start", 409)
                    await rpc(client, recovered, record.id, "cleanup")
                    require(
                        (await resources.get(recovered, record.id)).state
                        in {"removed", "abandoned"},
                        "Orphan remains",
                    )
                    job = await control.finish_cleanup(recovered)
                    require(
                        job.status == "failed"
                        and job.error_code == "INTERRUPTED"
                        and job.attempts == 1,
                        "Interrupted outcome incorrect",
                    )
                # Static IDs remain live and identical throughout all destructive dynamic checks.
                await check_outputs(
                    database=database, client=client, backend=backend, engine=engine
                )
                final = await Lifecycle(database, engine, backend).enroll()
                require(
                    final.definition == deployment.definition,
                    "Static deployment changed during cleanup",
                )
        finally:
            await stop(process)
            await engine.close()
            await backend.client.aclose()
    print("Restricted proxy, actual work, fenced cleanup and three process-crash windows passed")


if __name__ == "__main__":
    asyncio.run(crash_create() if len(sys.argv) > 1 else main())

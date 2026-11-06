"""Real project-scoped proxy/lifecycle and process-crash checks in a disposable schema."""

import asyncio
import os
import sys
from dataclasses import replace
from http import HTTPStatus
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
from scripts.check_resource_probe import check_resource_probe
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
                if (
                    await client.get(url="http://127.0.0.1:8081/health")
                ).status_code == HTTPStatus.OK.value:
                    return process
            except httpx.HTTPError:
                pass
            await asyncio.sleep(delay=0.1)
    await stop(process=process)
    raise RuntimeError("Test proxy did not become ready")


async def stop(process: asyncio.subprocess.Process, *, crash: bool = False) -> None:
    if process.returncode is None:
        if crash:
            process.kill()
        else:
            process.terminate()
    async with asyncio.timeout(delay=55):
        output, _error = await process.communicate()
    require(value=len(output) < 32768, message="Proxy log bound exceeded")
    require(
        value=not any(word in output.upper() for word in (b"WARNING", b"ERROR", b"TRACEBACK")),
        message="Proxy emitted a diagnostic during lifecycle verification",
    )


async def rpc(
    client: httpx.AsyncClient,
    lease: ControlLease,
    identifier: UUID,
    action: str,
    status: int = HTTPStatus.OK.value,
) -> None:
    reference = LeaseRef(
        run_id=lease.run_id, fence=lease.fence, owner=lease.owner, phase=lease.phase
    )
    response = await client.post(
        url=f"/control/resources/{identifier}/{action}", content=reference.model_dump_json()
    )
    require(
        value=response.status_code == status,
        message=f"Lifecycle {action} returned {response.status_code}: {response.text}",
    )


async def enqueue(database: Database) -> ControlLease:
    engine, http = Engine(), BackendHttp()
    try:
        deployment = await Lifecycle(database=database, engine=engine, http=http).enroll()
    finally:
        await engine.close()
        await http.client.aclose()
    logical = await prepare(database=database)
    await DockerCatalog(database=database).enqueue(
        snapshot=DockerSnapshot(
            dataset_id=logical.dataset.id,
            dataset_digest=logical.dataset.digest,
            request_count=logical.dataset.request_count,
            latest_arrival=logical.dataset.latest_arrival,
            deployment_id=deployment.id,
            deployment_digest=deployment.definition.digest(),
            configuration=DockerSettings(),
        )
    )
    lease = await DockerControl(database=database).claim(owner="lifecycle-check", lease_seconds=120)
    if lease is None:
        raise RuntimeError("Test Docker claim failed")
    return lease


async def crash_create() -> None:
    """Abrupt exit after real daemon success, optionally after persisting its immutable ID."""
    schema, run_id, identifier, fence, phase = sys.argv[2:]
    database = Database(url=os.environ["TEST_DATABASE_URL"], schema=schema)
    lease = ControlLease(
        run_id=UUID(hex=run_id),
        fence=int(fence),
        owner="lifecycle-check",
        expires_at=LeaseRef(
            run_id=UUID(hex=run_id), fence=int(fence), owner="lifecycle-check", phase="running"
        )
        .lease()
        .expires_at,
        phase="running",
    )
    resources, engine = DockerResources(database=database), Engine()
    intent = await resources.authorize_create(lease=lease, resource_id=UUID(hex=identifier))
    created = await engine.create(intent=intent)
    require(value=not bool(created.Warnings), message="Create emitted daemon diagnostics")
    if phase == "observed":
        http = BackendHttp()
        await Lifecycle(database=database, engine=engine, http=http).observe(
            lease=lease, identifier=UUID(hex=identifier)
        )
    os._exit(
        status=73
    )  # Deliberate process death: no Python finalizers/ID receipt in the creating case.


async def main() -> None:
    await check_resource_probe()
    # The actual installed service accepts only its own scoped credential; no raw API route.
    scoped = ControlClient()
    try:
        deployment = await scoped.enroll()
        require(
            value=deployment.definition.network_id != "",
            message="Installed proxy enrollment failed",
        )
    finally:
        await scoped.close()
    async with scratch(retain_on_failure=True) as database:
        resources, control = DockerResources(database=database), DockerControl(database=database)
        process = await server(database=database)
        engine, backend = Engine(), BackendHttp()
        auth = {"Authorization": "Bearer " + read_token(), "Content-Type": "application/json"}
        try:
            async with httpx.AsyncClient(
                base_url="http://127.0.0.1:8081", headers=auth, trust_env=False, timeout=50
            ) as client:
                require(
                    value=(await client.post(url="/containers/create", json={})).status_code
                    == HTTPStatus.NOT_FOUND.value,
                    message="Raw API exposed",
                )
                require(
                    value=(
                        await client.post(
                            url="/control/enroll",
                            json={},
                            headers={"Authorization": "Bearer invalid"},
                        )
                    ).status_code
                    == HTTPStatus.UNAUTHORIZED.value,
                    message="Bad token accepted",
                )
                lease = await enqueue(database=database)
                static = await resources.reserve_static(lease=lease, name="backend-a")
                record = await resources.prepare_dynamic(lease=lease)
                owned_id: str | None = None
                try:
                    await rpc(
                        client=client,
                        lease=replace(lease, fence=lease.fence + 1),
                        identifier=record.id,
                        action="create",
                        status=HTTPStatus.CONFLICT.value,
                    )
                    await rpc(client=client, lease=lease, identifier=record.id, action="create")
                    owned = await resources.get(lease=lease, resource_id=record.id)
                    owned_id = owned.container_id
                    require(
                        value=owned_id is not None and owned.state == "observed",
                        message="ID not saved before start",
                    )
                    await rpc(
                        client=client,
                        lease=lease,
                        identifier=record.id,
                        action="create",
                        status=HTTPStatus.CONFLICT.value,
                    )
                    await rpc(client=client, lease=lease, identifier=record.id, action="start")
                    spec = BackendSpec(id=record.name)
                    router = Router(specs=(spec,), strategy="first_fit", http=backend)
                    for _ in range(40):
                        await router.refresh()
                        if router.states[spec.id].healthy:
                            break
                        await asyncio.sleep(delay=0.1)
                    result = await backend.work(
                        reservation=router.reserve(
                            request=WorkRequest(request_id="lifecycle-real", work_units=1, mem_mb=1)
                        )
                    )
                    require(
                        value=result.backend_id == record.name and result.operations == 2048,
                        message="Actual work was not executed",
                    )
                    await rpc(
                        client=client,
                        lease=lease,
                        identifier=static.id,
                        action="start",
                        status=HTTPStatus.CONFLICT.value,
                    )
                finally:
                    cleanup = await control.begin_cleanup(
                        lease=lease, outcome="failed", error_code="CONTROLLER_FAILURE"
                    )
                    await rpc(client=client, lease=cleanup, identifier=record.id, action="cleanup")
                    await rpc(client=client, lease=cleanup, identifier=static.id, action="cleanup")
                    await control.finish_cleanup(lease=cleanup)
                if owned_id is None:
                    raise RuntimeError("Container ID missing")
                try:
                    await engine.inspect(reference=owned_id)
                except EngineError as error:
                    require(
                        value=error.status == HTTPStatus.NOT_FOUND.value,
                        message="Removed container absence not verified",
                    )
                else:
                    raise RuntimeError("Owned container survived cleanup")
                # Before create, after daemon success, and after ID persistence: never replay work.
                for phase in ("prepared", "creating", "observed"):
                    lease = await enqueue(database=database)
                    record = await resources.prepare_dynamic(lease=lease)
                    if phase != "prepared":
                        child = await asyncio.create_subprocess_exec(
                            sys.executable,
                            "-m",
                            "scripts.check_lifecycle",
                            "crash-create",
                            database.schema,
                            str(object=lease.run_id),
                            str(object=record.id),
                            str(object=lease.fence),
                            phase,
                        )
                        require(
                            value=await child.wait() == 73,
                            message="Fault process did not reach intended crash window",
                        )
                    await stop(process=process, crash=True)
                    await expire(database=database)
                    recovered = await control.claim(owner="recovery", lease_seconds=120)
                    require(
                        value=recovered is not None and recovered.phase == "cleanup",
                        message="Interrupted work was replayed",
                    )
                    if recovered is None:
                        raise RuntimeError("Cleanup claim missing")
                    process = await server(database=database)
                    await rpc(
                        client=client,
                        lease=lease,
                        identifier=record.id,
                        action="start",
                        status=HTTPStatus.CONFLICT.value,
                    )
                    await rpc(
                        client=client, lease=recovered, identifier=record.id, action="cleanup"
                    )
                    require(
                        value=(await resources.get(lease=recovered, resource_id=record.id)).state
                        in {"removed", "abandoned"},
                        message="Orphan remains",
                    )
                    job = await control.finish_cleanup(lease=recovered)
                    require(
                        value=job.status == "failed"
                        and job.error_code == "INTERRUPTED"
                        and job.attempts == 1,
                        message="Interrupted outcome incorrect",
                    )
                # Static IDs remain live and identical throughout all destructive dynamic checks.
                await check_outputs(
                    database=database, client=client, backend=backend, engine=engine
                )
                final = await Lifecycle(database=database, engine=engine, http=backend).enroll()
                require(
                    value=final.definition == deployment.definition,
                    message="Static deployment changed during cleanup",
                )
        finally:
            await stop(process=process)
            await engine.close()
            await backend.client.aclose()
    print("Restricted proxy, actual work, fenced cleanup and three process-crash windows passed")


if __name__ == "__main__":
    asyncio.run(main=crash_create() if len(sys.argv) > 1 else main())

"""Real private read-only resource evidence and strict credential-role separation."""

import asyncio
import os
from datetime import UTC, datetime
from http import HTTPStatus

from mslb_application.resource_policy import ResourcePolicy
from mslb_contracts.resources import ResourceProbe, safe_to_admit
from mslb_controller.control_client import ControlClient
from mslb_controller.engine import EngineError
from mslb_controller.resource_client import ResourceClient
from mslb_persistence.database import Database
from mslb_persistence.resource_evidence import ResourceEvidenceStore


async def diagnostic_probe(*, guard: ResourceClient) -> ResourceProbe:
    """Allow a finite diagnostic retry only for the private sampler's busy response."""
    for attempt in range(4):
        try:
            return await guard.probe()
        except EngineError as error:
            if error.status != HTTPStatus.TOO_MANY_REQUESTS.value or attempt == 3:
                raise
            await asyncio.sleep(delay=0.2)
    raise RuntimeError("Diagnostic measurement retry exhausted")


async def check_resource_probe() -> None:
    guard, controller = ResourceClient(), ControlClient()
    try:
        probe = await diagnostic_probe(guard=guard)
        current = [sample for sample in probe.services if sample.container_id is not None]
        if len(current) < 8 or not all(
            sample.healthy and sample.limit_bytes > 0 for sample in current
        ):
            raise RuntimeError("Default services did not produce healthy bounded resource evidence")
        if probe.available_memory_bytes <= 0 or probe.engine_memory_bytes < 8 * 1024**3:
            raise RuntimeError("Engine memory evidence does not meet the supported envelope")
        decision = ResourcePolicy().evaluate(probe=probe, now=datetime.now(tz=UTC))
        if len(current) < 10 and (decision.admit or decision.reason != "MISSING_SERVICE"):
            raise RuntimeError("Missing controller/guard services were incorrectly admitted")
        database = Database(url=os.environ["TEST_DATABASE_URL"])
        try:
            report = await ResourceEvidenceStore(database=database).read()
            if (
                report.received_at is None
                or (
                    report.decision.admit
                    and (report.probe is None or not safe_to_admit(probe=report.probe))
                )
                or (not report.decision.admit and report.decision.reason is None)
            ):
                raise RuntimeError(
                    "Default guard did not publish fresh consistent resource evidence"
                )
            service = next(item for item in probe.services if item.service == "resource-guard")
            if service.container_id is None or service.limit_bytes != 128 * 1024**2:
                raise RuntimeError("Default guard identity/cap was not measured")
        finally:
            await database.close()
        for path in ("/control/enroll", "/containers/create"):
            response = await guard.client.post(url=path, json={})
            if response.status_code != HTTPStatus.UNAUTHORIZED.value:
                raise RuntimeError("Read-only credential reached a mutation/control endpoint")
        response = await controller.client.get(url="/guard/probe")
        if response.status_code != HTTPStatus.UNAUTHORIZED.value:
            raise RuntimeError("Controller credential crossed the guard role boundary")
    finally:
        await guard.close()
        await controller.close()
    print("Read-only resource evidence and isolated credential roles passed")

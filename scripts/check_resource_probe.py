"""Real private read-only resource evidence and strict credential-role separation."""

import os
from datetime import UTC, datetime

from mslb_application.resource_policy import ResourcePolicy
from mslb_controller.control_client import ControlClient
from mslb_controller.resource_client import ResourceClient
from mslb_persistence.database import Database
from mslb_persistence.resource_evidence import ResourceEvidenceStore


async def check_resource_probe() -> None:
    guard, controller = ResourceClient(), ControlClient()
    try:
        probe = await guard.probe()
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
            if report.probe is None or report.decision.admit or not report.decision.stop_running:
                raise RuntimeError(
                    "Default guard did not publish a fresh explicit readiness refusal"
                )
            service = next(
                item for item in report.probe.services if item.service == "resource-guard"
            )
            if service.container_id is None or service.limit_bytes != 128 * 1024**2:
                raise RuntimeError("Default guard identity/cap was not measured")
        finally:
            await database.close()
        for path in ("/control/enroll", "/containers/create"):
            response = await guard.client.post(url=path, json={})
            if response.status_code != 401:
                raise RuntimeError("Read-only credential reached a mutation/control endpoint")
        response = await controller.client.get(url="/guard/probe")
        if response.status_code != 401:
            raise RuntimeError("Controller credential crossed the guard role boundary")
    finally:
        await guard.close()
        await controller.close()
    print("Read-only resource evidence and isolated credential roles passed")

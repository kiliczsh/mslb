"""Readiness reflects actual current guarded SQL evidence and fails closed on expiry."""

import asyncio
from datetime import timedelta
from http import HTTPStatus

import httpx
from fastapi import FastAPI
from mslb_api.routes_health import register_health
from mslb_persistence.models import ResourceEvidence
from mslb_persistence.resource_evidence import ResourceEvidenceStore
from sqlalchemy import func, update

from tests.integration.helpers import scratch
from tests.integration.test_resource_evidence import healthy_evidence


def test_installation_readiness_requires_current_safe_guarded_evidence() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            database.enforce_resource_guard = True
            app = FastAPI()
            register_health(app=app, database=database)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://testserver"
            ) as client:
                missing = await client.get(url="/api/v1/health/ready")
                assert missing.status_code == HTTPStatus.SERVICE_UNAVAILABLE.value
                assert not missing.json()["ready"] and not missing.json()["complete_product"]
                store = ResourceEvidenceStore(database=database)
                lease = await store.claim(owner="readiness-test")
                assert lease is not None
                probe, decision = healthy_evidence()
                await store.publish(
                    lease=lease, expected_revision=lease.revision, probe=probe, decision=decision
                )
                ready = await client.get(url="/api/v1/health/ready")
                assert ready.status_code == HTTPStatus.OK.value
                assert ready.json()["ready"] and ready.json()["complete_product"]
                assert ready.json()["capabilities"] == {
                    "logical_rest": "ready",
                    "mcp": "ready",
                    "dashboard": "ready",
                    "docker_traffic": "ready",
                    "resource_guard": "ready",
                    "telemetry": "deferred",
                }
                async with database.engine.begin() as connection:
                    await connection.execute(
                        statement=update(table=ResourceEvidence).values(
                            received_at=func.clock_timestamp() - timedelta(seconds=4)
                        )
                    )
                stale = await client.get(url="/api/v1/health/ready")
                assert stale.status_code == HTTPStatus.SERVICE_UNAVAILABLE.value
                assert not stale.json()["ready"] and not stale.json()["complete_product"]
                assert stale.json()["capabilities"]["docker_traffic"] == "unavailable"
                assert stale.json()["resource_admission"]["reason"] == "MEASUREMENTS_STALE"

    asyncio.run(main=scenario())

"""Managed operational snapshots are durable before a Docker job can be registered."""

import asyncio
import hashlib
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_runs import DockerSettings, DockerSnapshot
from mslb_persistence.catalog import Catalog
from mslb_persistence.docker_catalog import DockerCatalog
from mslb_persistence.records import RunRecord
from mslb_persistence.resource_evidence import ResourceEvidenceStore

from mslb_application.admission import MaterializationAdmission
from mslb_application.docker_snapshots import MAX_SNAPSHOT_BYTES as MAX_SNAPSHOT_BYTES
from mslb_application.docker_snapshots import load_docker_snapshot as load_docker_snapshot
from mslb_application.docker_snapshots import save_docker_snapshot as save_docker_snapshot
from mslb_application.errors import OperationError
from mslb_application.managed_files import ManagedFiles


class DockerSubmissions:
    """Freeze trusted operational inputs and enqueue durable Docker work."""

    def __init__(self, *, catalog: Catalog, docker: DockerCatalog, files: ManagedFiles) -> None:
        self.catalog, self.docker, self.files = catalog, docker, files
        self.admission = MaterializationAdmission()

    async def submit(
        self,
        *,
        dataset_id: UUID,
        settings: DockerSettings,
        deployment_id: UUID,
        key: str | None = None,
        scope: str = "local",
    ) -> RunRecord:
        """Serialize bounded operational materialization and preserve idempotent identity."""
        checked = DockerSettings.model_validate(obj=settings.model_dump())
        digest = hashlib.sha256(
            string=canonical_bytes(
                value={
                    "dataset_id": str(object=dataset_id),
                    "settings": checked.model_dump(mode="json"),
                }
            )
        ).hexdigest()
        existing = await self.catalog.find_idempotent(
            key=key, body_digest=digest, scope=scope, kind="docker"
        )
        if existing is not None:
            return existing
        return await self.admission.run(
            operation=lambda: self._submit(
                dataset_id=dataset_id,
                settings=checked,
                deployment_id=deployment_id,
                key=key,
                scope=scope,
            )
        )

    async def _submit(
        self,
        *,
        dataset_id: UUID,
        settings: DockerSettings,
        deployment_id: UUID,
        key: str | None,
        scope: str,
    ) -> RunRecord:
        """Freeze trusted operational inputs and enqueue only a matching dataset."""
        if self.catalog.database.enforce_resource_guard:
            evidence = await ResourceEvidenceStore(database=self.catalog.database).read()
            if not evidence.decision.admit:
                raise OperationError(
                    code="RESOURCE_PRESSURE", message="Fresh safe resource evidence is required"
                )
        dataset = await self.catalog.get_dataset(dataset_id=dataset_id)
        # The adapter supplies a server-enrolled deployment, never caller daemon definitions.
        deployment = await self.docker.deployment(deployment_id=deployment_id)
        requests = await asyncio.to_thread(
            self.files.load_requests, digest=dataset.digest, size_bytes=dataset.size_bytes
        )
        if len(requests) != dataset.request_count or requests[-1].t != dataset.latest_arrival:
            raise OperationError(
                code="VALIDATION_FAILED", message="Docker dataset metadata differs"
            )
        snapshot = DockerSnapshot(
            dataset_id=dataset.id,
            dataset_digest=dataset.digest,
            request_count=dataset.request_count,
            latest_arrival=dataset.latest_arrival,
            deployment_id=deployment.id,
            deployment_digest=deployment.definition.digest(),
            configuration=settings,
        )
        await asyncio.to_thread(save_docker_snapshot, files=self.files, snapshot=snapshot)
        return await self.docker.enqueue(snapshot=snapshot, key=key, scope=scope)

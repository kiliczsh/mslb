"""Managed operational snapshots are durable before a Docker job can be registered."""

import asyncio
import hashlib
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_runs import DockerSettings, DockerSnapshot
from mslb_persistence.catalog import Catalog
from mslb_persistence.docker_catalog import DockerCatalog
from mslb_persistence.records import RunRecord
from pydantic import ValidationError

from mslb_application.admission import MaterializationAdmission
from mslb_application.errors import OperationError
from mslb_application.files import FileReference, ManagedFiles

MAX_SNAPSHOT_BYTES = 16 * 1024


def save_docker_snapshot(*, files: ManagedFiles, snapshot: DockerSnapshot) -> FileReference:
    # Frozen models can still be constructed with an unchecked model_copy update.
    checked = DockerSnapshot.model_validate(obj=snapshot.model_dump())
    return files.publish(
        key=f"docker/snapshots/{checked.digest()}.json",
        chunks=(canonical_bytes(value=checked.model_dump(mode="json")),),
        maximum=MAX_SNAPSHOT_BYTES,
    )


def load_docker_snapshot(*, files: ManagedFiles, key: str, expected_digest: str) -> DockerSnapshot:
    if key != f"docker/snapshots/{expected_digest}.json":
        raise OperationError(code="VALIDATION_FAILED", message="Docker snapshot namespace differs")
    try:
        with files.path(key=key).open(mode="rb") as stream:
            raw = stream.read(MAX_SNAPSHOT_BYTES + 1)
        if (
            len(raw) > MAX_SNAPSHOT_BYTES
            or hashlib.sha256(string=raw).hexdigest() != expected_digest
        ):
            raise OperationError(code="VALIDATION_FAILED", message="Docker snapshot bytes differ")
        snapshot = DockerSnapshot.model_validate_json(json_data=raw)
        if (
            snapshot.digest() != expected_digest
            or canonical_bytes(value=snapshot.model_dump(mode="json")) != raw
        ):
            raise OperationError(
                code="VALIDATION_FAILED", message="Docker snapshot identity differs"
            )
        return snapshot
    except (OSError, ValidationError):
        raise OperationError(
            code="VALIDATION_FAILED", message="Docker snapshot is unavailable or invalid"
        ) from None


class DockerSubmissions:
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
        checked = DockerSettings.model_validate(obj=settings.model_dump())
        digest = hashlib.sha256(
            string=canonical_bytes(
                value={"dataset_id": str(dataset_id), "settings": checked.model_dump(mode="json")}
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

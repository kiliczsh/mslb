"""Physical output bytes, replayed accounting, scope and immutable resource histories."""

import asyncio
import hashlib
import json
from dataclasses import dataclass, replace
from pathlib import Path
from uuid import uuid4

import pytest
from mslb_application.docker_outputs import DockerOutputs, verify_docker_outputs
from mslb_application.errors import OperationError
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_runs import (
    DockerManifest,
    DockerSettings,
    DockerSnapshot,
    StaticBackend,
    StaticReservation,
)
from mslb_contracts.docker_runtime import HttpEvent
from mslb_contracts.durability import StoredFile
from mslb_contracts.inputs import RequestInput, input_digest
from mslb_controller.routing import BackendSpec
from mslb_controller.traffic import TrafficDriver
from mslb_persistence.docker_records import ResourceRecord

from tests.test_traffic_driver import FakeClock, Transport, rows


@dataclass(frozen=True, slots=True, kw_only=True)
class Candidate:
    files: ManagedFiles
    manifest: DockerManifest
    snapshot: DockerSnapshot
    requests: tuple[RequestInput, ...]


async def candidate(
    *,
    root: Path,
    release: bool = True,
    configuration: DockerSettings | None = None,
    count: int = 2,
    delay_kind: str | None = None,
) -> Candidate:
    requests = rows(count=count)
    snapshot = DockerSnapshot(
        dataset_id=uuid4(),
        dataset_digest=input_digest(records=requests),
        request_count=len(requests),
        latest_arrival=0,
        deployment_id=uuid4(),
        deployment_digest="a" * 64,
        configuration=configuration or DockerSettings(initial_replicas=1, duration_seconds=1),
    )
    run_id, resource_id = uuid4(), uuid4()
    definition = StaticReservation(
        deployment_id=snapshot.deployment_id,
        run_id=run_id,
        resource_id=resource_id,
        slot=0,
        backend=StaticBackend(name="backend-a", container_id="b" * 64),
    )
    resource = ResourceRecord(
        id=resource_id,
        ordinal=1,
        slot=0,
        kind="static",
        state="observed",
        name="backend-a",
        container_id="b" * 64,
        definition=definition,
    )
    files = ManagedFiles(root=root, limits=StorageLimits(min_free_bytes=0))
    http = Transport()
    try:
        async with DockerOutputs(
            files=files, run_id=run_id, fence=3, snapshot=snapshot, requests=requests
        ) as output:
            await output.observe(resource=resource)
            clock = FakeClock()

            class DelayedSink:
                async def append(self, *, event: HttpEvent) -> None:
                    await output.append(event=event)
                    if event.kind == delay_kind:
                        clock.current += 1

            driver = TrafficDriver(
                configuration=snapshot.configuration,
                specs=(BackendSpec(id="backend-a"),),
                http=http,
                sink=DelayedSink(),
                clock=clock,
            )
            summary = await driver.execute(requests=requests)
            if release:
                await output.observe(resource=replace(resource, state="released"))
            manifest = await output.finalize(summary=summary)
            return Candidate(files=files, manifest=manifest, snapshot=snapshot, requests=requests)
    finally:
        await http.close()


def verify(*, saved: Candidate, manifest: DockerManifest | None = None) -> None:
    runtime = verify_docker_outputs(
        files=saved.files,
        manifest=manifest or saved.manifest,
        snapshot=saved.snapshot,
        requests=saved.requests,
    )
    assert runtime.traffic.completed == runtime.traffic.total == 2
    assert runtime.lifecycle_events == 2


def test_actual_files_round_trip_scope_accounting_and_private_spool_cleanup(tmp_path: Path) -> None:
    saved = asyncio.run(main=candidate(root=tmp_path))
    verify(saved=saved)
    assert set(saved.manifest.artifacts) == {
        "http_events.jsonl",
        "runtime_summary.json",
        "container_lifecycle.jsonl",
    }
    for reference in saved.manifest.artifacts.values():
        assert saved.files.path(key=reference.key).is_file()
        assert reference.key.startswith(f"docker/{saved.manifest.run_id.hex}/3/")
    assert list(tmp_path.rglob(pattern="*.spool")) == []
    assert not (tmp_path / "run.jsonl").exists() and not (tmp_path / "runs").exists()
    assert saved.files.run_scope(key="docker/snapshots/abc.json") is None
    assert (
        saved.files.run_scope(key=saved.manifest.artifacts["http_events.jsonl"].key)
        == f"docker/{saved.manifest.run_id.hex}"
    )


@pytest.mark.parametrize(
    argnames="mutation",
    argvalues=["scope", "sequence", "backend", "request", "duplicate", "operations"],
)
def test_rehashed_http_tampering_is_rejected_independently(tmp_path: Path, mutation: str) -> None:
    saved = asyncio.run(main=candidate(root=tmp_path))
    original = saved.manifest.artifacts["http_events.jsonl"]
    path = saved.files.path(key=original.key)
    events = [json.loads(s=line) for line in path.read_bytes().splitlines()]
    if mutation == "scope":
        events[0]["run_id"] = str(object=uuid4())
    elif mutation == "sequence":
        events[0]["sequence"] = 2
    elif mutation == "backend":
        for event in events:
            if event["backend_id"] is not None:
                event["backend_id"] = "backend-b"
            if event["result"] is not None:
                event["result"]["backend_id"] = "backend-b"
    elif mutation == "request":
        events[0]["request_id"] = "foreign"
    elif mutation == "duplicate":
        events[-1] = events[-2] | {"sequence": len(events)}
    else:
        events[-1]["result"]["operations"] = 2047
    raw = b"".join(
        (json.dumps(obj=event, sort_keys=True, separators=(",", ":")) + "\n").encode()
        for event in events
    )
    path.write_bytes(data=raw)
    changed = StoredFile(
        key=original.key, sha256=hashlib.sha256(string=raw).hexdigest(), size_bytes=len(raw)
    )
    manifest = saved.manifest.model_copy(
        update={"artifacts": saved.manifest.artifacts | {"http_events.jsonl": changed}}
    )
    with pytest.raises(expected_exception=OperationError):
        verify(saved=saved, manifest=manifest)


def test_unresolved_resource_prevents_verified_publication_and_spools_are_closed(
    tmp_path: Path,
) -> None:
    with pytest.raises(expected_exception=OperationError, match="unresolved"):
        asyncio.run(main=candidate(root=tmp_path, release=False))
    assert list(tmp_path.rglob(pattern="*.spool")) == []


@pytest.mark.parametrize(
    argnames="mutation", argvalues=["foreign_definition", "changed_id", "unreleased"]
)
def test_rehashed_lifecycle_identity_and_unresolved_history_are_rejected(
    tmp_path: Path, mutation: str
) -> None:
    saved = asyncio.run(main=candidate(root=tmp_path))
    original = saved.manifest.artifacts["container_lifecycle.jsonl"]
    path = saved.files.path(key=original.key)
    events = [json.loads(s=line) for line in path.read_bytes().splitlines()]
    if mutation == "foreign_definition":
        events[-1]["definition"]["deployment_id"] = str(object=uuid4())
    elif mutation == "changed_id":
        events[-1]["container_id"] = "c" * 64
    else:
        events[-1]["state"] = "observed"
    raw = b"".join(
        (json.dumps(obj=event, sort_keys=True, separators=(",", ":")) + "\n").encode()
        for event in events
    )
    path.write_bytes(data=raw)
    changed = StoredFile(
        key=original.key, sha256=hashlib.sha256(string=raw).hexdigest(), size_bytes=len(raw)
    )
    manifest = saved.manifest.model_copy(
        update={"artifacts": saved.manifest.artifacts | {"container_lifecycle.jsonl": changed}}
    )
    with pytest.raises(expected_exception=OperationError):
        verify(saved=saved, manifest=manifest)


def test_hash_mismatch_and_scoped_candidate_quota_are_enforced(tmp_path: Path) -> None:
    saved = asyncio.run(main=candidate(root=tmp_path))
    reference = saved.manifest.artifacts["runtime_summary.json"]
    saved.files.path(key=reference.key).write_bytes(data=b"corrupt")
    with pytest.raises(expected_exception=OperationError, match="identity mismatch"):
        verify(saved=saved)
    root = tmp_path / "quota"
    files = ManagedFiles(root=root, limits=StorageLimits(quota_bytes=32, min_free_bytes=0))
    key = f"docker/{uuid4().hex}/1/.http.spool"
    with (
        pytest.raises(expected_exception=OperationError, match="quota"),
        files.work_file(key=key) as stream,
    ):
        stream.write(data=b"x" * 33)
        stream.sync()
    assert not files.path(key=key).exists()


@pytest.mark.parametrize(argnames="field", argvalues=["peak_pending", "peak_inflight"])
@pytest.mark.parametrize(argnames="direction", argvalues=["lower", "higher"])
def test_rehashed_summary_cannot_understate_or_overstate_observed_peaks(
    tmp_path: Path, field: str, direction: str
) -> None:
    saved = asyncio.run(main=candidate(root=tmp_path))
    original = saved.manifest.artifacts["runtime_summary.json"]
    path = saved.files.path(key=original.key)
    runtime = json.loads(s=path.read_bytes())
    assert runtime["traffic"]["peak_pending"] == 1
    assert runtime["traffic"]["peak_inflight"] == 2
    runtime["traffic"][field] = 0 if direction == "lower" else runtime["traffic"][field] + 1
    raw = canonical_bytes(value=runtime)
    path.write_bytes(data=raw)
    changed = StoredFile(
        key=original.key, sha256=hashlib.sha256(string=raw).hexdigest(), size_bytes=len(raw)
    )
    manifest = saved.manifest.model_copy(
        update={"artifacts": saved.manifest.artifacts | {"runtime_summary.json": changed}}
    )
    with pytest.raises(expected_exception=OperationError, match="observed peaks"):
        verify(saved=saved, manifest=manifest)


@pytest.mark.parametrize(argnames="variant", argvalues=["overflow", "arrived", "admitted"])
def test_peak_replay_accounts_for_overflow_and_reserved_but_unsent_work(
    tmp_path: Path, variant: str
) -> None:
    saved = asyncio.run(
        main=candidate(
            root=tmp_path,
            configuration=DockerSettings(
                initial_replicas=1,
                duration_seconds=1,
                global_inflight=1,
                backend_inflight=1,
                max_pending=1,
            ),
            count=4 if variant == "overflow" else 2,
            delay_kind=None if variant == "overflow" else variant,
        )
    )
    runtime = verify_docker_outputs(
        files=saved.files,
        manifest=saved.manifest,
        snapshot=saved.snapshot,
        requests=saved.requests,
    )
    assert runtime.traffic.peak_pending == 1
    assert runtime.traffic.peak_inflight == (0 if variant == "arrived" else 1)
    if variant == "overflow":
        assert runtime.traffic.completed == 2 and runtime.traffic.dropped == 2
    else:
        assert runtime.traffic.unexecuted == 2 and runtime.traffic.completed == 0

"""Real filesystem/SQL orphan retention protects active and published application data."""

import asyncio
import hashlib
import os
from pathlib import Path
from uuid import uuid4

import pytest
from mslb_application.errors import OperationError
from mslb_application.file_maintenance import FileMaintenance
from mslb_application.managed_files import ManagedFiles
from mslb_application.storage_values import StorageLimits
from mslb_application.worker import Worker
from mslb_contracts.runs import RunSettings
from mslb_persistence.durability import Durability

from tests.integration.helpers import scratch
from tests.integration.test_queries import prepare


def old_file(*, files: ManagedFiles, key: str) -> Path:
    files.publish(key=key, chunks=(b"orphan",), maximum=6)
    path = files.path(key=key)
    os.utime(path=path, times=(0, 0))
    return path


def test_idle_bounded_orphan_maintenance_preserves_published_bytes_and_recent_files(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            files, service, runs = await prepare(
                database=database, root=tmp_path, settings=(RunSettings(),)
            )
            maintenance = FileMaintenance(database=database, files=files)
            old = old_file(files=files, key=f"runs/{uuid4().hex}/1/orphan.jsonl")
            with pytest.raises(expected_exception=OperationError, match="idle"):
                await maintenance.run(apply=True)
            assert old.exists()
            assert await Worker(database=database, inputs=service.files, outputs=files).run_once()
            pointer = await Durability(database=database).published(run_id=runs[0])
            published = files.path(key=pointer.key)
            before = hashlib.sha256(string=published.read_bytes()).hexdigest()
            os.utime(path=published, times=(0, 0))
            historical = old_file(files=files, key=f"runs/{runs[0]}/retained-private-history.jsonl")
            recent = files.publish(
                key=f"docker/{uuid4().hex}/1/recent.jsonl", chunks=(b"new",), maximum=3
            )
            plan = await maintenance.run(limit=1)
            assert plan.dry_run and plan.candidates == 1 and plan.deleted == 0 and old.exists()
            done = await maintenance.run(apply=True, limit=1)
            assert (
                not done.dry_run
                and done.deleted == 1
                and done.reclaimed_bytes == 6
                and not old.exists()
            )
            assert historical.exists() and files.path(key=recent.key).exists()
            assert (
                hashlib.sha256(string=published.read_bytes()).hexdigest()
                == before
                == pointer.sha256
            )
            assert (await Durability(database=database).published(run_id=runs[0])) == pointer

    asyncio.run(main=scenario())


def test_maintenance_rejects_symlinks_before_any_removal(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            files = ManagedFiles(root=tmp_path / "outputs", limits=StorageLimits(min_free_bytes=0))
            old = old_file(files=files, key=f"runs/{uuid4().hex}/1/orphan.jsonl")
            outside = tmp_path / "outside"
            outside.write_bytes(data=b"preserved")
            link = files.root / "unsafe"
            link.symlink_to(target=outside)
            with pytest.raises(expected_exception=OperationError, match="unsafe"):
                await FileMaintenance(database=database, files=files).run(apply=True)
            assert outside.read_bytes() == b"preserved" and old.exists() and link.is_symlink()

    asyncio.run(main=scenario())

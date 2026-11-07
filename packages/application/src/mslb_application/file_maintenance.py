"""Bounded idle maintenance removes only old, unreferenced project output files."""

import asyncio
import errno
import heapq
import os
import stat
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from mslb_persistence.database import Database
from mslb_persistence.file_retention import FileRetention, RetentionSnapshot

from mslb_application.admission import settle
from mslb_application.errors import OperationError
from mslb_application.managed_files import ManagedFiles


@dataclass(frozen=True, slots=True, kw_only=True)
class MaintenanceReport:
    """Report an exact bounded batch, without deleting history or input datasets."""

    dry_run: bool
    candidates: int
    deleted: int
    reclaimed_bytes: int
    protected_scopes: int
    referenced_files: int


class FileMaintenance:
    """Preserve all published scopes and committed references; refuse active maintenance."""

    def __init__(self, *, database: Database, files: ManagedFiles) -> None:
        self.database, self.files = database, files

    async def run(
        self, *, apply: bool = False, limit: int = 500, min_age_seconds: int = 3600
    ) -> MaintenanceReport:
        """Plan by default; mutate only a bounded old-file batch under global writer admission."""
        if (
            type(apply) is not bool
            or type(limit) is not int
            or not 1 <= limit <= 500
            or type(min_age_seconds) is not int
            or not 3600 <= min_age_seconds <= 30 * 86400
        ):
            raise OperationError(code="VALIDATION_ERROR", message="Invalid maintenance bounds")
        with self.files._lock(name=".write-lock", wait=False):
            snapshot = await FileRetention(database=self.database).snapshot()
            if snapshot.active:
                raise OperationError(
                    code="RATE_LIMITED",
                    message="Output maintenance requires idle execution and resolved cleanup",
                )
            task = asyncio.create_task(
                coro=asyncio.to_thread(
                    self._scan,
                    snapshot=snapshot,
                    apply=apply,
                    limit=limit,
                    min_age_seconds=min_age_seconds,
                )
            )
            return await settle(task=task)

    def _scan(
        self, *, snapshot: RetentionSnapshot, apply: bool, limit: int, min_age_seconds: int
    ) -> MaintenanceReport:
        if sys.platform != "linux":
            raise RuntimeError("Output maintenance requires Linux containers")
        self.files._usage()  # Validate count/depth and all entry kinds before any removal.
        for reference in snapshot.references:
            self.files.path(key=reference)
        cutoff = time.time() - min_age_seconds
        candidates: list[tuple[float, str, int, int, int]] = []
        for directory, _children, names in os.walk(top=self.files.root, followlinks=False):
            for name in names:
                path = Path(directory) / name
                key = path.relative_to(self.files.root).as_posix()
                staging = path.parent == self.files.root / ".staging"
                scope = self.files.run_scope(key=key)
                if (
                    (not staging and scope is None)
                    or scope in snapshot.published
                    or key in snapshot.references
                ):
                    continue
                metadata = path.lstat()
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_uid != os.getuid()
                    or metadata.st_nlink != 1
                ):
                    raise OperationError(
                        code="VALIDATION_FAILED",
                        message="Maintenance found unsafe file ownership or links",
                    )
                if metadata.st_mtime <= cutoff:
                    candidates.append(
                        (metadata.st_mtime, key, metadata.st_size, metadata.st_ino, metadata.st_dev)
                    )
        selected = heapq.nsmallest(n=limit, iterable=candidates)
        reclaimed = 0
        if apply:
            for _modified, key, size, inode, device in selected:
                path = self.files.root / key
                if not path.resolve().is_relative_to(self.files.root):
                    raise OperationError(
                        code="VALIDATION_FAILED",
                        message="Maintenance target escapes managed storage",
                    )
                current = path.lstat()
                if (
                    current.st_ino != inode
                    or current.st_dev != device
                    or current.st_size != size
                    or current.st_mtime > cutoff
                    or not stat.S_ISREG(current.st_mode)
                    or current.st_uid != os.getuid()
                    or current.st_nlink != 1
                ):
                    raise OperationError(
                        code="CONFLICT", message="Maintenance target changed after planning"
                    )
                path.unlink()
                reclaimed += size
                parent = path.parent
                while parent != self.files.root and parent != self.files.root / ".staging":
                    try:
                        parent.rmdir()
                    except OSError as error:
                        if error.errno in {errno.ENOTEMPTY, errno.EEXIST, errno.ENOENT}:
                            break
                        raise
                    parent = parent.parent
            self.files._sync_directory(directory=self.files.root)
        return MaintenanceReport(
            dry_run=not apply,
            candidates=len(selected),
            deleted=len(selected) if apply else 0,
            reclaimed_bytes=reclaimed,
            protected_scopes=len(snapshot.published),
            referenced_files=len(snapshot.references),
        )

"""Immutable managed-storage budgets and references."""

import re
from dataclasses import dataclass
from pathlib import Path

from mslb_application.errors import OperationError


@dataclass(frozen=True, slots=True)
class StorageLimits:
    """Bound physical managed bytes, free-space reserve and directory entry count."""

    quota_bytes: int = 5 * 1024**3
    min_free_bytes: int = 2 * 1024**3
    max_entries: int = 100_000

    def __post_init__(self) -> None:
        if any(
            type(value) is not int or value < 0
            for value in (self.quota_bytes, self.min_free_bytes, self.max_entries)
        ) or not (1 <= self.quota_bytes <= 5 * 1024**3 and 1 <= self.max_entries <= 100_000):
            raise OperationError("VALIDATION_ERROR", "Invalid managed storage limits")


@dataclass(frozen=True, slots=True)
class FileReference:
    """Bind a server-derived storage key to its exact byte length and SHA-256."""

    key: str
    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        if (
            not re.fullmatch(r"[0-9a-f]{64}", self.sha256)
            or type(self.size_bytes) is not int
            or not 0 <= self.size_bytes <= 256 * 1024**2
        ):
            raise OperationError("VALIDATION_ERROR", "Invalid managed file reference")


@dataclass(frozen=True, slots=True)
class _Staged:
    path: Path
    sha256: str
    size_bytes: int

"""Validated configuration for one private work backend."""

import os
import re
from dataclasses import dataclass
from typing import Self


@dataclass(frozen=True, slots=True)
class Settings:
    """Bound private backend identity, CPU work, admission and per-call deadlines."""

    backend_id: str
    logical_admission_mem_mb: int = 1024
    max_inflight: int = 4

    def __post_init__(self) -> None:
        """Reject inconsistent service access or execution bounds at construction."""
        if not re.fullmatch(pattern=r"[a-z][a-z0-9-]{0,127}", string=self.backend_id):
            raise ValueError("Invalid backend identity")
        if (
            type(self.logical_admission_mem_mb) is not int
            or not 1 <= self.logical_admission_mem_mb <= 1_000_000_000
            or type(self.max_inflight) is not int
            or not 1 <= self.max_inflight <= 4
        ):
            raise ValueError("Invalid backend limits")

    @classmethod
    def environment(cls) -> Self:
        """Load and validate the service's explicit environment policy."""
        return cls(
            backend_id=os.environ["BACKEND_ID"],
            logical_admission_mem_mb=int(
                os.environ.get(key="LOGICAL_ADMISSION_MEM_MB", default="1024")
            ),
            max_inflight=int(os.environ.get(key="MAX_INFLIGHT", default="4")),
        )

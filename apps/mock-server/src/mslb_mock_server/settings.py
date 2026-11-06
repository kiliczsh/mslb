"""Validated configuration for one private work backend."""

import os
import re
from dataclasses import dataclass
from typing import Self


@dataclass(frozen=True, slots=True)
class Settings:
    backend_id: str
    logical_admission_mem_mb: int = 1024
    max_inflight: int = 4

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,127}", self.backend_id):
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
        return cls(
            backend_id=os.environ["BACKEND_ID"],
            logical_admission_mem_mb=int(os.environ.get("LOGICAL_ADMISSION_MEM_MB", "1024")),
            max_inflight=int(os.environ.get("MAX_INFLIGHT", "4")),
        )

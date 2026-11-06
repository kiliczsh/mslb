"""Authorized internal download handles with verified stream ownership."""

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import BinaryIO

from mslb_application.managed_files import ManagedFiles
from mslb_application.storage_values import FileReference


@dataclass(frozen=True, slots=True)
class Download:
    """An internal handle; adapters must authorize the run before calling Queries."""

    files: ManagedFiles
    reference: FileReference
    filename: str
    media_type: str

    @contextmanager
    def open(self) -> Generator[BinaryIO]:
        # Verify before sending headers; adapters stream this open file in 64 KiB
        # chunks and close it on disconnect. No caller-provided path is accepted.
        with self.files.verified_stream(reference=self.reference) as stream:
            yield stream

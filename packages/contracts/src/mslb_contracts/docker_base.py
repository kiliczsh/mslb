"""Strict Docker provenance values shared by operational contracts."""

import hashlib
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from mslb_contracts.canonical import canonical_bytes

type DockerId = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


type ImageId = Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]


class DockerModel(BaseModel):
    """Provide strict immutable operational contracts distinct from logical outputs."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    def digest(self) -> str:
        """Hash the canonical JSON representation of this frozen operational contract."""
        return hashlib.sha256(data=canonical_bytes(value=self.model_dump(mode="json"))).hexdigest()

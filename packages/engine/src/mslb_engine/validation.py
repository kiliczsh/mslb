"""Domain bounds and canonical identifier validation without I/O."""

import unicodedata

from mslb_engine.errors import SimulationError


def _request_limit(*, value: int) -> None:
    """Keep standard and explicit stress populations finite without external dependencies."""
    if type(value) is not int or value not in {10_000, 50_000}:
        raise SimulationError(code="VALIDATION_ERROR", message="Unsupported request capacity")


def _integer(value: int, minimum: int, maximum: int) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise SimulationError(
            code="VALIDATION_ERROR", message="Integer is outside the supported bounds"
        )


def _identifier(value: str) -> None:
    if not isinstance(value, str) or not 1 <= len(value.encode(encoding="utf-8")) <= 128:
        raise SimulationError(code="VALIDATION_ERROR", message="Invalid identifier")
    if unicodedata.normalize("NFC", value.strip()) != value:
        raise SimulationError(
            code="VALIDATION_ERROR", message="Input identifiers must already be normalized"
        )

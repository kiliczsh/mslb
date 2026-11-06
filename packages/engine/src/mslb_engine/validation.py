"""Domain bounds and canonical identifier validation without I/O."""

import unicodedata

from mslb_engine.errors import SimulationError


def _integer(value: int, minimum: int, maximum: int) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise SimulationError("VALIDATION_ERROR", "Integer is outside the supported bounds")


def _identifier(value: str) -> None:
    if not isinstance(value, str) or not 1 <= len(value.encode("utf-8")) <= 128:
        raise SimulationError("VALIDATION_ERROR", "Invalid identifier")
    if unicodedata.normalize("NFC", value.strip()) != value:
        raise SimulationError("VALIDATION_ERROR", "Input identifiers must already be normalized")

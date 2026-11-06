"""Small redacted failure records; canonical artifacts remain the execution authority."""

import logging
from contextlib import suppress
from typing import Literal
from uuid import UUID

LOGGER = logging.getLogger(name=__name__)


def report_failure(
    *,
    error: Exception,
    code: Literal["RESOURCE_PRESSURE", "INTERNAL_ERROR"],
    source: Literal["rest", "application", "worker_poll"],
    request_id: str = "unavailable",
) -> None:
    """Record bounded type/category/UUID fields without exception text or traceback."""
    identifier = "unavailable"
    if len(request_id) <= 36:
        with suppress(ValueError):
            identifier = str(object=UUID(hex=request_id))
    kind = (
        "".join(
            character
            for character in type(error).__name__[:64]
            if character.isascii() and (character.isalnum() or character == "_")
        )
        or "Exception"
    )
    level = logging.WARNING if code == "RESOURCE_PRESSURE" else logging.ERROR
    LOGGER.log(
        level,
        "operation_failed source=%s code=%s error_type=%s request_id=%s",
        source,
        code,
        kind,
        identifier,
    )

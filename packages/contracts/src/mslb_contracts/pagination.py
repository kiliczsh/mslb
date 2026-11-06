"""Bounded shared collection pages."""

from typing import Annotated

from pydantic import Field

from mslb_contracts.read_base import ReadModel


class Page[T](ReadModel):
    """Return a bounded collection and optional opaque continuation cursor."""

    items: Annotated[tuple[T, ...], Field(max_length=500)]
    next_cursor: Annotated[str, Field(max_length=2048)] | None = None

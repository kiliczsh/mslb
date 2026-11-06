"""Bounded REST pagination and logical tick query types."""

from typing import Annotated

from fastapi import Query

type Limit = Annotated[int, Query(ge=1, le=500)]

type Cursor = Annotated[str | None, Query(max_length=2048)]

type Tick = Annotated[int, Query(ge=0, le=1_000_000)]

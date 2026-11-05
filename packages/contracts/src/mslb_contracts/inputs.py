"""Bounded, normalized input contracts shared by every application adapter."""

import csv
import hashlib
import json
import re
import unicodedata
from collections.abc import Iterator
from typing import Annotated, BinaryIO, override

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

MAX_REQUESTS = 10_000
MAX_UPLOAD_BYTES = 10 * 1024 * 1024
MAX_ROW_BYTES = 2048
MAX_FIELD_BYTES = 256
MAX_VALUE = 1_000_000_000
PositiveValue = Annotated[int, Field(strict=True, ge=1, le=MAX_VALUE)]
ArrivalTick = Annotated[int, Field(strict=True, ge=0, lt=1_000_000)]


class InputError(ValueError):
    """A safe field-specific diagnostic that does not echo uploaded content."""

    def __init__(self, field: str, reason: str) -> None:
        self.field = field
        self.reason = reason
        super().__init__(f"{field}: {reason}")


def normalize_identifier(value: str) -> str:
    """Trim edge whitespace and normalize Unicode without folding case."""
    normalized = unicodedata.normalize("NFC", value.strip())
    try:
        size = len(normalized.encode("utf-8"))
    except UnicodeEncodeError:
        raise ValueError("invalid_utf8") from None
    if not 1 <= size <= 128:
        raise ValueError("identifier_must_be_1_to_128_utf8_bytes")
    return normalized


class ServerInput(BaseModel):
    """A static logical server specification; not an ORM or managed version."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    id: Annotated[str, Field(max_length=256)]
    cpu_units_per_tick: PositiveValue
    mem_mb: PositiveValue
    rate_limit_per_sec: PositiveValue

    @field_validator("id")
    @classmethod
    def normalize_id(cls, value: str) -> str:
        return normalize_identifier(value)


class RequestInput(BaseModel):
    """An immutable normalized arrival record."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    t: ArrivalTick
    request_id: Annotated[str, Field(max_length=256)]
    work_units: PositiveValue
    mem_mb: PositiveValue

    @field_validator("request_id")
    @classmethod
    def normalize_id(cls, value: str) -> str:
        return normalize_identifier(value)


class ServerDocument(BaseModel):
    """The assignment's JSON document with a bounded server collection."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    tick_seconds: Annotated[int, Field(strict=True, ge=1, le=1)] = 1
    servers: Annotated[tuple[ServerInput, ...], Field(min_length=1, max_length=64)]

    @field_validator("servers")
    @classmethod
    def order_unique_servers(cls, value: tuple[ServerInput, ...]) -> tuple[ServerInput, ...]:
        if len({server.id for server in value}) != len(value):
            raise ValueError("duplicate_normalized_identifier")
        return tuple(sorted(value, key=lambda server: server.id))


def read_servers(stream: BinaryIO) -> tuple[ServerInput, ...]:
    """Load bounded JSON, accepting a UTF-8 BOM and rejecting malformed input."""
    payload = stream.read(256 * 1024 + 1)
    if len(payload) > 256 * 1024:
        raise InputError("servers", "document_too_large")
    try:
        document = ServerDocument.model_validate_json(payload.decode("utf-8-sig"))
    except UnicodeDecodeError:
        raise InputError("servers", "invalid_utf8") from None
    except ValidationError as error:
        first = error.errors(include_input=False)[0]
        field = ".".join(str(part) for part in first["loc"]) or "servers"
        raise InputError(field, first["type"]) from None
    return document.servers


class _BoundedLines(Iterator[str]):
    def __init__(self, stream: BinaryIO) -> None:
        self.stream = stream
        self.total = 0
        self.row_bytes = 0

    @override
    def __next__(self) -> str:
        raw = self.stream.readline(MAX_ROW_BYTES + 1)
        if not raw:
            raise StopIteration
        first = self.total == 0
        self.total += len(raw)
        self.row_bytes += len(raw)
        if self.total > MAX_UPLOAD_BYTES:
            raise InputError("csv", "upload_too_large")
        if self.row_bytes > MAX_ROW_BYTES:
            raise InputError("csv", "row_too_large")
        try:
            return raw.decode("utf-8-sig" if first else "utf-8")
        except UnicodeDecodeError:
            raise InputError("csv", "invalid_utf8") from None


def _integer(value: str, field: str) -> int:
    if len(value) > 10 or not re.fullmatch(r"[0-9]+", value):
        raise InputError(field, "expected_bounded_integer")
    return int(value)


def read_requests(stream: BinaryIO) -> tuple[RequestInput, ...]:
    """Parse CSV incrementally, bounding physical and quoted logical rows."""
    lines = _BoundedLines(stream)
    reader = csv.reader(lines, strict=True)
    try:
        header = next(reader, [])
        required = ("t", "request_id", "work_units", "mem_mb")
        if len(set(header)) != len(header) or not set(required).issubset(header):
            raise InputError("header", "missing_or_duplicate_columns")
        if any(len(field.encode("utf-8")) > MAX_FIELD_BYTES for field in header):
            raise InputError("header", "field_too_large")
        indexes = {name: header.index(name) for name in required}
        requests: list[RequestInput] = []
        seen: set[str] = set()
        while True:
            lines.row_bytes = 0
            row = next(reader, None)
            if row is None:
                break
            field = f"rows[{len(requests) + 2}]"
            if len(requests) >= MAX_REQUESTS:
                raise InputError(field, "request_count_exceeded")
            if len(row) != len(header):
                raise InputError(field, "row_width")
            if any(len(cell.encode("utf-8")) > MAX_FIELD_BYTES for cell in row):
                raise InputError(field, "field_too_large")
            try:
                request = RequestInput(
                    t=_integer(row[indexes["t"]], field + ".t"),
                    request_id=row[indexes["request_id"]],
                    work_units=_integer(row[indexes["work_units"]], field + ".work_units"),
                    mem_mb=_integer(row[indexes["mem_mb"]], field + ".mem_mb"),
                )
            except ValidationError as error:
                first = error.errors(include_input=False)[0]
                suffix = ".".join(str(part) for part in first["loc"])
                raise InputError(field + "." + suffix, first["type"]) from None
            if request.request_id in seen:
                raise InputError(field + ".request_id", "duplicate_normalized_identifier")
            seen.add(request.request_id)
            requests.append(request)
    except csv.Error:
        raise InputError("csv", "malformed_row") from None
    if not requests:
        raise InputError("csv", "empty_workload")
    return tuple(sorted(requests, key=lambda request: (request.t, request.request_id)))


def input_digest(records: tuple[ServerInput, ...] | tuple[RequestInput, ...]) -> str:
    """Hash already-normalized ordered records with explicit schema identity."""
    digest = hashlib.sha256(b"load-balancer-input-v1\n")
    for record in records:
        digest.update(
            json.dumps(
                record.model_dump(), sort_keys=True, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()

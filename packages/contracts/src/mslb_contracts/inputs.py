"""Bounded, normalized input contracts shared by every application adapter."""

import csv
import hashlib
import json
import re
import unicodedata
from collections.abc import Iterator
from typing import Annotated, BinaryIO, override

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from mslb_contracts.capacity_profiles import validate_request_limit

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


def normalize_identifier(*, value: str) -> str:
    """Trim edge whitespace and normalize Unicode without folding case."""
    normalized = unicodedata.normalize("NFC", value.strip())
    if "\x00" in normalized:
        raise ValueError("identifier_cannot_contain_nul")
    if normalized in {".", ".."}:
        raise ValueError("identifier_cannot_be_dot_segment")
    try:
        size = len(normalized.encode(encoding="utf-8"))
    except UnicodeEncodeError:
        raise ValueError("invalid_utf8") from None
    if not 1 <= size <= 128:
        raise ValueError("identifier_must_be_1_to_128_utf8_bytes")
    return normalized


class ServerInput(BaseModel):
    """A static logical server specification; not an ORM or managed version."""

    model_config = ConfigDict(
        strict=True,
        frozen=True,
        extra="forbid",
        json_schema_extra={
            "examples": [
                {"id": "edge-a", "cpu_units_per_tick": 8, "mem_mb": 64, "rate_limit_per_sec": 4}
            ]
        },
    )
    id: Annotated[str, Field(max_length=256)]
    cpu_units_per_tick: PositiveValue
    mem_mb: PositiveValue
    rate_limit_per_sec: PositiveValue

    @field_validator("id")
    @classmethod
    def normalize_id(cls, value: str) -> str:
        """Normalize and validate a server identifier at the input boundary."""
        return normalize_identifier(value=value)


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
        """Normalize and validate a request identifier at the input boundary."""
        return normalize_identifier(value=value)


class ServerDocument(BaseModel):
    """The assignment's JSON document with a bounded server collection."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    tick_seconds: Annotated[int, Field(strict=True, ge=1, le=1)] = 1
    servers: Annotated[tuple[ServerInput, ...], Field(min_length=1, max_length=64)]

    @field_validator("servers")
    @classmethod
    def order_unique_servers(cls, value: tuple[ServerInput, ...]) -> tuple[ServerInput, ...]:
        """Sort servers deterministically and reject duplicate identifiers."""
        if len({server.id for server in value}) != len(value):
            raise ValueError("duplicate_normalized_identifier")
        return tuple(sorted(value, key=lambda server: server.id))


def read_servers(stream: BinaryIO) -> tuple[ServerInput, ...]:
    """Load bounded JSON, accepting a UTF-8 BOM and rejecting malformed input."""
    payload = stream.read(256 * 1024 + 1)
    if len(payload) > 256 * 1024:
        raise InputError(field="servers", reason="document_too_large")
    try:
        document = ServerDocument.model_validate_json(
            json_data=payload.decode(encoding="utf-8-sig")
        )
    except UnicodeDecodeError:
        raise InputError(field="servers", reason="invalid_utf8") from None
    except ValidationError as error:
        first = error.errors(include_input=False)[0]
        field = ".".join(str(object=part) for part in first["loc"]) or "servers"
        raise InputError(field=field, reason=first["type"]) from None
    return document.servers


class _BoundedLines(Iterator[str]):
    """Decode CSV lines while enforcing field/row budgets before parser allocation."""

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
            raise InputError(field="csv", reason="upload_too_large")
        if self.row_bytes > MAX_ROW_BYTES:
            raise InputError(field="csv", reason="row_too_large")
        try:
            return raw.decode(encoding="utf-8-sig" if first else "utf-8")
        except UnicodeDecodeError:
            raise InputError(field="csv", reason="invalid_utf8") from None


def _integer(value: str, field: str) -> int:
    if len(value) > 10 or not re.fullmatch(pattern=r"[0-9]+", string=value):
        raise InputError(field=field, reason="expected_bounded_integer")
    return int(value)


def read_requests(
    stream: BinaryIO, *, max_requests: int = MAX_REQUESTS
) -> tuple[RequestInput, ...]:
    """Parse CSV incrementally, bounding physical and quoted logical rows."""
    validate_request_limit(value=max_requests)
    lines = _BoundedLines(stream=stream)
    reader = csv.reader(lines, strict=True)
    try:
        header = next(reader, [])
        required = ("t", "request_id", "work_units", "mem_mb")
        if len(set(header)) != len(header) or not set(required).issubset(header):
            raise InputError(field="header", reason="missing_or_duplicate_columns")
        if any(len(field.encode(encoding="utf-8")) > MAX_FIELD_BYTES for field in header):
            raise InputError(field="header", reason="field_too_large")
        indexes = {name: header.index(name) for name in required}
        requests: list[RequestInput] = []
        seen: set[str] = set()
        while True:
            lines.row_bytes = 0
            row = next(reader, None)
            if row is None:
                break
            field = f"rows[{len(requests) + 2}]"
            if len(requests) >= max_requests:
                raise InputError(field=field, reason="request_count_exceeded")
            if len(row) != len(header):
                raise InputError(field=field, reason="row_width")
            if any(len(cell.encode(encoding="utf-8")) > MAX_FIELD_BYTES for cell in row):
                raise InputError(field=field, reason="field_too_large")
            try:
                request = RequestInput(
                    t=_integer(value=row[indexes["t"]], field=field + ".t"),
                    request_id=row[indexes["request_id"]],
                    work_units=_integer(
                        value=row[indexes["work_units"]], field=field + ".work_units"
                    ),
                    mem_mb=_integer(value=row[indexes["mem_mb"]], field=field + ".mem_mb"),
                )
            except ValidationError as error:
                first = error.errors(include_input=False)[0]
                suffix = ".".join(str(object=part) for part in first["loc"])
                raise InputError(field=field + "." + suffix, reason=first["type"]) from None
            if request.request_id in seen:
                raise InputError(
                    field=field + ".request_id", reason="duplicate_normalized_identifier"
                )
            seen.add(request.request_id)
            requests.append(request)
    except csv.Error:
        raise InputError(field="csv", reason="malformed_row") from None
    if not requests:
        raise InputError(field="csv", reason="empty_workload")
    return tuple(sorted(requests, key=lambda request: (request.t, request.request_id)))


def input_digest(records: tuple[ServerInput, ...] | tuple[RequestInput, ...]) -> str:
    """Hash already-normalized ordered records with explicit schema identity."""
    # This versioned wire-format marker must stay stable for archived snapshot identities.
    digest = hashlib.sha256(data=b"load-balancer-input-v1\n")
    for record in records:
        digest.update(
            json.dumps(
                obj=record.model_dump(), sort_keys=True, ensure_ascii=False, separators=(",", ":")
            ).encode(encoding="utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()

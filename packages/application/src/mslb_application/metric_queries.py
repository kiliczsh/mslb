"""Streaming metric artifact selection and exact compatible-run comparisons."""

import hashlib
from collections.abc import Generator, Iterator
from fractions import Fraction
from typing import Annotated, BinaryIO, cast

import ijson
from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.fractions import FractionValue
from mslb_contracts.inputs import ServerInput
from mslb_contracts.queries import JsonValue, ReadModel, RunView, ServerMetricView
from mslb_engine.metrics import ChartSample, ComparisonIdentity, compare_values
from pydantic import Field, TypeAdapter

from mslb_application.errors import OperationError

JSON = TypeAdapter(JsonValue)


def exact(value: object) -> Fraction:
    if type(value) is int and abs(value) <= 2**53 - 1:
        return Fraction(value)
    return FractionValue.model_validate(value).to_fraction()


def metric_report(
    stream: BinaryIO, server_ids: tuple[str, ...]
) -> tuple[dict[str, JsonValue], dict[str, JsonValue]]:
    # ijson's untyped parser is confined to this adapter. Validate decoded values
    # before they enter the typed result; do not materialize the entire server map.
    """Read exact selected metrics without materializing unselected server histories."""

    def summary_events() -> Generator[tuple[str, str, object]]:
        events = cast(Iterator[tuple[str, str, object]], ijson.parse(stream))
        for prefix, event, value in events:
            if prefix == "servers" or prefix.startswith("servers."):
                continue
            if (prefix, event, value) != ("", "map_key", "servers"):
                yield prefix, event, value

    members = cast(Iterator[tuple[str, object]], ijson.kvitems(summary_events(), ""))
    summary = {key: JSON.validate_python(value) for key, value in members}
    servers: dict[str, JsonValue] = {}
    if server_ids:
        stream.seek(0)
        members = cast(Iterator[tuple[str, object]], ijson.kvitems(stream, "servers"))
        for index, (key, value) in enumerate(members):
            if index >= 10_000:
                raise OperationError("VALIDATION_FAILED", "Metric history exceeds its series limit")
            if key in server_ids:
                servers[key] = JSON.validate_python(value)
        if set(servers) != set(server_ids):
            raise OperationError("NOT_FOUND", "Requested metric server is not in this run")
    return summary, servers


def server_metric_page(
    stream: BinaryIO, after: str | None, limit: int
) -> tuple[ServerMetricView, ...]:
    """Read a stable bounded server-metric page from verified saved JSON."""
    members = cast(Iterator[tuple[str, object]], ijson.kvitems(stream, "servers"))
    selected: list[ServerMetricView] = []
    previous: str | None = None
    for index, (key, value) in enumerate(members):
        if index >= 10_000 or (previous is not None and key <= previous):
            raise OperationError(
                "VALIDATION_FAILED", "Metric history order or population is invalid"
            )
        previous = key
        if after is None or key > after:
            selected.append(
                ServerMetricView(
                    id=key, metrics=TypeAdapter(dict[str, JsonValue]).validate_python(value)
                )
            )
            if len(selected) == limit:
                break
    return tuple(selected)


type WireNumber = Annotated[int, Field(ge=0, le=2**53 - 1)] | FractionValue


class TimelineSample(ReadModel):
    """Decode one bounded saved server sample for result queries."""

    server_id: str
    active_ids: Annotated[tuple[str, ...], Field(max_length=10_000)]
    reserved_mem_mb: Annotated[int, Field(ge=0, le=64 * 10**9)]
    capacity: Annotated[int, Field(ge=1, le=10**9)]
    memory_capacity: Annotated[int, Field(ge=1, le=10**9)]
    starts: Annotated[int, Field(ge=0, le=10_000)]
    allocated_cpu: WireNumber
    consumed_cpu: WireNumber


class TimelineFrame(ReadModel):
    """Decode saved interval membership and request IDs for persisted playback."""

    t: Annotated[int, Field(ge=0, le=1_000_000)]
    duration_ticks: Annotated[int, Field(ge=0, le=1_000_000)]
    terminal: bool
    pending_ids: Annotated[tuple[str, ...], Field(max_length=10_000)]
    membership: Annotated[tuple[ServerInput, ...], Field(max_length=64)]
    servers: Annotated[tuple[TimelineSample, ...], Field(max_length=64)]


def chart_samples(stream: BinaryIO, metric: str, server_id: str | None) -> Generator[ChartSample]:
    """Yield exact saved samples for one selected metric and optional server."""
    found = server_id is None
    for index in range(1_000_002):
        line = stream.readline(8 * 1024**2 + 1)
        if not line:
            break
        if index > 1_000_000 or len(line) > 8 * 1024**2 or not line.endswith(b"\n"):
            raise OperationError("VALIDATION_FAILED", "Timeline exceeds supported frame bounds")
        frame = TimelineFrame.model_validate_json(line)
        if server_id is not None and any(server.id == server_id for server in frame.membership):
            found = True
        if frame.terminal:
            continue
        samples = [sample for sample in frame.servers if server_id in {None, sample.server_id}]
        if server_id is not None and not samples:
            continue
        if metric == "pending_depth":
            value = Fraction(len(frame.pending_ids))
        elif metric == "active_requests":
            value = Fraction(sum(len(sample.active_ids) for sample in samples))
        elif metric == "cpu_utilization":
            capacity = sum(sample.capacity for sample in samples)
            value = (
                sum((exact(sample.consumed_cpu) for sample in samples), Fraction()) / capacity
                if capacity
                else Fraction()
            )
        else:
            capacity = sum(sample.memory_capacity for sample in samples)
            value = Fraction(sum(sample.reserved_mem_mb for sample in samples), capacity or 1)
        yield ChartSample(frame.t, frame.duration_ticks, value)
    if not found:
        raise OperationError("NOT_FOUND", "Requested chart server is not in this run")


def comparison_identity(run: RunView) -> ComparisonIdentity:
    settings = run.configuration.model_dump(mode="json", exclude={"strategy", "autoscaling"})
    return ComparisonIdentity(
        run.dataset_digest,
        run.server_snapshot_digest,
        run.configuration.mode,
        run.engine_version,
        hashlib.sha256(canonical_bytes(settings)).hexdigest(),
    )


def compare_reports(
    baseline: RunView,
    candidate: RunView,
    first: dict[str, JsonValue],
    second: dict[str, JsonValue],
) -> dict[str, JsonValue]:
    """Compare exact completed reports using compatible frozen identities."""
    identity, other = comparison_identity(baseline), comparison_identity(candidate)
    # Validate identity even when a report contains only null latency values.
    compare_values(identity, other, Fraction(), Fraction())
    result: dict[str, JsonValue] = {}

    def visit(left: dict[str, JsonValue], right: dict[str, JsonValue], prefix: str = "") -> None:
        if set(left) != set(right):
            raise OperationError("VALIDATION_FAILED", "Metric report schemas differ")
        for key, value in left.items():
            name, counterpart = prefix + key, right[key]
            if isinstance(value, dict) and set(value) != {"numerator", "denominator"}:
                if not isinstance(counterpart, dict):
                    raise OperationError("VALIDATION_FAILED", "Metric report schemas differ")
                visit(value, counterpart, name + ".")
            elif value is None or counterpart is None:
                result[name] = {
                    "baseline": value,
                    "candidate": counterpart,
                    "difference": None,
                    "percentage_difference": None,
                }
            else:
                result[name] = compare_values(identity, other, exact(value), exact(counterpart))

    visit(first, second)
    return result

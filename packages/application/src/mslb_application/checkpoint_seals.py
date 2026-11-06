"""Terminal output sealing without replaying terminal boundaries."""

import hashlib
from collections.abc import Mapping
from typing import BinaryIO

from mslb_contracts.durability import StoredFile, TerminalSeal
from mslb_contracts.runs import RunSnapshot
from mslb_engine.metrics import MetricAccumulator
from mslb_engine.simulator import Simulation
from pydantic import ValidationError

from mslb_application.checkpoint_prefixes import _require, _scan_prefix
from mslb_application.checkpoint_values import PREFIX_NAMES
from mslb_application.errors import OperationError
from mslb_application.snapshots import domain_config, domain_inputs, verify_snapshot


def capture_terminal_seal(
    snapshot: RunSnapshot,
    simulation: Simulation,
    metrics: MetricAccumulator,
    streams: Mapping[str, BinaryIO],
    report: StoredFile,
) -> TerminalSeal:
    """Seal final prefixes without inventing a resumable terminal interval."""
    verify_snapshot(snapshot)
    initial, requests = domain_inputs(snapshot)
    _require(
        simulation.initial_servers == initial
        and simulation.requests == requests
        and simulation.config == domain_config(snapshot.configuration)
        and metrics.requests == simulation.by_id
        and report.size_bytes > 0,
        "Terminal execution differs from its frozen snapshot",
    )
    _require(
        simulation.terminal and metrics.terminal and simulation.tick == metrics.next_tick,
        "Terminal seal requires a fully observed terminal boundary",
    )
    _require(
        len(metrics.finishes) + len(metrics.drops) == len(simulation.requests)
        and len(metrics.arrivals) == len(simulation.requests),
        "Terminal request accounting is incomplete",
    )
    _require(set(streams) == set(PREFIX_NAMES), "All four terminal prefixes are required")
    prefixes = tuple(_scan_prefix(name, streams[name]) for name in PREFIX_NAMES)
    counts = {prefix.name: prefix.records for prefix in prefixes}
    _require(
        counts["run.jsonl"] == simulation.event_count
        and counts["autoscaling_decisions.jsonl"] == simulation.decision_count
        and counts["timeline.jsonl"] == simulation.executed_intervals + 1
        and counts["drop_reasons.jsonl"] == len(metrics.drops),
        "Terminal prefix counts disagree with execution",
    )
    return TerminalSeal(
        snapshot_digest=snapshot.snapshot_digest,
        engine_version=snapshot.engine_version,
        terminal_tick=simulation.tick,
        executed_intervals=simulation.executed_intervals,
        prefixes=prefixes,
        metrics=report,
    )


def read_terminal_seal(
    stream: BinaryIO, expected_digest: str, snapshot: RunSnapshot
) -> TerminalSeal:
    """Read finalization identity only; never restore/replay a terminal boundary."""
    verify_snapshot(snapshot)
    payload = stream.read(1024**2 + 1)
    _require(
        len(payload) <= 1024**2 and hashlib.sha256(payload).hexdigest() == expected_digest,
        "Terminal seal length or digest mismatch",
    )
    try:
        seal = TerminalSeal.model_validate_json(payload)
    except ValidationError:
        raise OperationError("CHECKPOINT_INVALID", "Invalid terminal seal schema") from None
    _require(
        seal.snapshot_digest == snapshot.snapshot_digest
        and seal.engine_version == snapshot.engine_version,
        "Terminal seal identity mismatch",
    )
    prefixes = {prefix.name: prefix for prefix in seal.prefixes}
    _require(
        set(prefixes) == set(PREFIX_NAMES)
        and prefixes["timeline.jsonl"].records == seal.executed_intervals + 1
        and seal.executed_intervals
        <= seal.terminal_tick
        <= snapshot.configuration.max_simulation_ticks,
        "Terminal seal boundary or prefix identities are invalid",
    )
    return seal

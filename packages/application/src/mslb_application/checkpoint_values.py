"""Checkpoint budgets, artifact references and restored execution values."""

from dataclasses import dataclass
from pathlib import Path

from mslb_contracts.checkpoints import (
    ArtifactPrefix,
    PrefixName,
)
from mslb_engine.metrics import MetricAccumulator
from mslb_engine.simulator import Simulation

# Checkpoints fit the worker's bounded materialization slot; prefixes share a
# separate aggregate output ceiling and exact arithmetic has a finite bit budget.
MAX_CHECKPOINT_BYTES = 32 * 1024 * 1024
MAX_PREFIX_BYTES = 512 * 1024**2
MAX_CHECKPOINT_FRACTION_BITS = 4096


PREFIX_NAMES: tuple[PrefixName, ...] = (
    "run.jsonl",
    "autoscaling_decisions.jsonl",
    "timeline.jsonl",
    "drop_reasons.jsonl",
)


@dataclass(frozen=True, slots=True)
class CheckpointArtifact:
    """Reference a flushed interval checkpoint and its exact committed prefixes."""

    path: Path
    sha256: str
    size_bytes: int
    next_tick: int
    executed_intervals: int
    event_count: int
    prefixes: tuple[ArtifactPrefix, ...]


@dataclass(frozen=True, slots=True)
class RestoredCheckpoint:
    """Return verified engine, metric and prefix state ready for safe continuation."""

    simulation: Simulation
    metrics: MetricAccumulator
    prefixes: tuple[ArtifactPrefix, ...]

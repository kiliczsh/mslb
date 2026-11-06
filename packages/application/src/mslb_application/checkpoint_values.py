"""Checkpoint budgets, artifact references and restored execution values."""

from dataclasses import dataclass
from pathlib import Path

from mslb_contracts.checkpoints import (
    ArtifactPrefix,
    PrefixName,
)
from mslb_engine.metrics import MetricAccumulator
from mslb_engine.simulator import Simulation

MAX_CHECKPOINT_BYTES = 32 * 1024 * 1024


PREFIX_NAMES: tuple[PrefixName, ...] = (
    "run.jsonl",
    "autoscaling_decisions.jsonl",
    "timeline.jsonl",
    "drop_reasons.jsonl",
)


@dataclass(frozen=True, slots=True)
class CheckpointArtifact:
    path: Path
    sha256: str
    size_bytes: int
    next_tick: int
    executed_intervals: int
    event_count: int
    prefixes: tuple[ArtifactPrefix, ...]


@dataclass(frozen=True, slots=True)
class RestoredCheckpoint:
    simulation: Simulation
    metrics: MetricAccumulator
    prefixes: tuple[ArtifactPrefix, ...]

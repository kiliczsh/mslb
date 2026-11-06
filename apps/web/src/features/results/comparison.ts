import type { Run } from "../../shared/api/contracts.js";

export function compatibleRuns(baseline: Run, candidate: Run) {
  const { strategy: _firstStrategy, autoscaling: _firstScaling, ...first } = baseline.configuration;
  const {
    strategy: _otherStrategy,
    autoscaling: _otherScaling,
    ...other
  } = candidate.configuration;
  return (
    baseline.status === "completed" &&
    candidate.status === "completed" &&
    baseline.dataset_digest === candidate.dataset_digest &&
    baseline.server_snapshot_digest === candidate.server_snapshot_digest &&
    baseline.engine_version === candidate.engine_version &&
    JSON.stringify(first) === JSON.stringify(other)
  );
}

import { displayValue, exactText } from "../../features/results/metricValues.js";
import type { MetricValue } from "../../features/results/resultContracts.js";

export function MetricNumber({ value }: { value: MetricValue | null }) {
  return <span title={`Exact: ${exactText(value)}`}>{displayValue(value)}</span>;
}

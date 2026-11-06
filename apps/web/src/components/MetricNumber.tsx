import { displayValue, exactText } from "../lib/metricValues.js";
import type { MetricValue } from "../lib/resultContracts.js";

export function MetricNumber({ value }: { value: MetricValue | null }) {
  return <span title={`Exact: ${exactText(value)}`}>{displayValue(value)}</span>;
}

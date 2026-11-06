import type { MetricValue } from "../../shared/api/resultContracts.js";
import { displayValue, exactText } from "./metricValues.js";

export function MetricNumber({ value }: { value: MetricValue | null }) {
  return <span title={`Exact: ${exactText(value)}`}>{displayValue(value)}</span>;
}

import { parseFraction } from "./fractions.js";
import type { MetricValue } from "./resultContracts.js";

function parts(value: MetricValue) {
  return typeof value === "number"
    ? { numerator: BigInt(value), denominator: 1n }
    : parseFraction(value);
}

export function exactText(value: MetricValue | null) {
  if (value === null) return "Not defined";
  const { numerator, denominator } = parts(value);
  return denominator === 1n ? numerator.toString() : `${numerator}/${denominator}`;
}

export function displayValue(value: MetricValue | null) {
  if (value === null) return "Not defined";
  const { numerator, denominator } = parts(value);
  if (denominator === 1n) return numerator.toString();
  const absolute = numerator < 0n ? -numerator : numerator;
  const rounded = (absolute * 20_000n + denominator) / (2n * denominator);
  const fraction = (rounded % 10_000n).toString().padStart(4, "0").replace(/0+$/, "");
  return `≈ ${numerator < 0n && rounded !== 0n ? "-" : ""}${rounded / 10_000n}${fraction ? `.${fraction}` : ""}`;
}

/** Chart conversion follows exact BigInt scaling, never Number(big)/Number(big). */
export function chartNumber(value: MetricValue) {
  const { numerator, denominator } = parts(value);
  const approximate = Number((numerator * 100_000_000n) / denominator) / 100_000_000;
  return Number.isFinite(approximate) ? approximate : null;
}

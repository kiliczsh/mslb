import type { Settings } from "./contracts.js";

/** Apply engine policy relationships after the strict settings schema succeeds. */
export function scalingPolicyError({
  policy,
  initialServerCount,
}: {
  policy: Settings["autoscaling"];
  initialServerCount: number;
}): string | null {
  for (const key of [
    "scale_out_cpu_threshold",
    "scale_in_cpu_threshold",
    "scale_out_drop_rate_threshold",
  ] as const) {
    const threshold = policy[key];
    const numerator = BigInt(threshold.numerator);
    if (numerator < 0n || numerator > BigInt(threshold.denominator))
      return `${key}: threshold must be between 0 and 1.`;
  }
  const low = policy.scale_in_cpu_threshold;
  const high = policy.scale_out_cpu_threshold;
  if (
    BigInt(low.numerator) * BigInt(high.denominator) >=
    BigInt(high.numerator) * BigInt(low.denominator)
  )
    return "Scale-in CPU threshold must be below scale-out CPU threshold.";
  if (policy.enabled && policy.server_template === null)
    return "Enabled simulated autoscaling requires an explicit server template.";
  if (
    (policy.min_servers ?? initialServerCount) > initialServerCount ||
    (policy.max_servers ?? Math.min(64, initialServerCount + 3)) < initialServerCount
  )
    return "Scaling server bounds must include the current initial server count.";
  return null;
}

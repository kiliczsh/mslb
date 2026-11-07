import type { ServerInput } from "../../shared/api/contracts.js";

export const clusterProfiles = [
  {
    id: "balanced",
    label: "Balanced",
    description: "Equal capacities for comparing scheduling strategies.",
  },
  {
    id: "cpu",
    label: "Uneven CPU",
    description: "Different CPU capacities with equal memory and start limits.",
  },
  {
    id: "memory",
    label: "Memory pressure",
    description: "Different memory capacities reveal eligibility and waiting.",
  },
  {
    id: "rate",
    label: "Rate limited",
    description: "One request start per server per tick, with ample CPU and memory.",
  },
] as const;
export type ClusterProfile = (typeof clusterProfiles)[number]["id"];

export function presetServers({
  count,
  profile,
}: {
  count: number;
  profile: ClusterProfile;
}): ServerInput[] {
  if (!Number.isInteger(count) || count < 3 || count > 5) throw new Error("Choose 3–5 servers.");
  const cpus = [2, 4, 8, 12, 16];
  const memory = [128, 256, 512, 1024, 2048];
  return Array.from({ length: count }, (_, index) => ({
    id: `s${index + 1}`,
    cpu_units_per_tick: profile === "cpu" ? (cpus[index] ?? 4) : profile === "rate" ? 16 : 4,
    mem_mb: profile === "memory" ? (memory[index] ?? 512) : profile === "rate" ? 1024 : 512,
    rate_limit_per_sec: profile === "rate" ? 1 : profile === "cpu" ? 3 : 4,
  }));
}

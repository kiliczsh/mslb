import { defaultSettings, type Server, type Settings } from "../../shared/api/contracts.js";

export const demoRecipes = [
  {
    id: "balanced",
    title: "Balanced arrivals",
    description: "A few arrivals at each tick. Follow assignments and completions.",
    preview:
      "Requests arrive in groups of three across successive ticks. Follow how they are assigned and how capacity becomes available as work finishes.",
  },
  {
    id: "sharing",
    title: "CPU sharing",
    description: "A simultaneous burst. Inspect how active work shares available CPU.",
    preview:
      "All requests arrive together at tick 0. Watch active requests share each server’s CPU and see how contention affects completion times.",
  },
  {
    id: "memory",
    title: "Memory squeeze",
    description: "Large reservations. Watch eligibility, waiting and memory release.",
    preview:
      "All requests arrive at tick 0, each reserving as much memory as the largest server can hold. Watch which servers can admit them and how waiting requests advance when memory is released.",
  },
] as const;
export type DemoKind = (typeof demoRecipes)[number]["id"];
export const demoStrategies = [
  "first_fit",
  "round_robin",
  "least_connections",
  "most_available_memory",
] as const;
export const strategyLabels: Record<Settings["strategy"], string> = {
  first_fit: "First Fit",
  round_robin: "Round Robin",
  least_connections: "Least Connections",
  most_available_memory: "Most Available Memory",
};
export const strategyDescriptions: Record<Settings["strategy"], string> = {
  first_fit:
    "Selects the first eligible server by ID. Later servers can stay idle while earlier servers have room.",
  round_robin:
    "Takes turns across eligible servers in ID order, wrapping around after the last server.",
  least_connections:
    "Selects the eligible server with the fewest active requests, breaking ties by server ID.",
  most_available_memory:
    "Selects the eligible server with the most unreserved memory, breaking ties by server ID.",
};

export type DemoSession = {
  generation: string;
  csv: string;
  settings: Settings;
  datasetId: string | null;
  attempts: { strategy: Settings["strategy"]; key: string; runId: string | null }[];
};
export type DemoDraft = { recipe: DemoKind; session: DemoSession | null };
export function createDemoDraft(): DemoDraft {
  return { recipe: "balanced", session: null };
}

/** Small deterministic workloads adapt to the current catalog without changing its servers. */
export function demoInput({ servers, kind }: { servers: readonly Server[]; kind: DemoKind }) {
  if (servers.length === 0 || servers.length > 64) throw new Error("Configure 1–64 servers first.");
  const count = Math.min(24, Math.max(6, servers.length * 3));
  const memory = Math.max(...servers.map((server) => server.mem_mb));
  const mem = kind === "memory" ? memory : 1;
  const cpu = Math.min(
    ...servers.filter((server) => server.mem_mb >= mem).map((server) => server.cpu_units_per_tick),
  );
  const work = Math.max(
    1,
    Math.min(1_000_000_000, kind === "balanced" ? Math.ceil(cpu / 2) : cpu * 2),
  );
  const rows = Array.from(
    { length: count },
    (_, index) =>
      `${kind === "balanced" ? Math.floor(index / 3) : 0},demo-${String(index + 1).padStart(3, "0")},${work},${mem}`,
  );
  return {
    csv: `t,request_id,work_units,mem_mb\n${rows.join("\n")}\n`,
    count,
    work,
    memory: mem,
    settings: { ...structuredClone(defaultSettings), mode: "concurrent" as const },
  };
}

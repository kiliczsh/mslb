import { useState } from "react";
import type { Api } from "../lib/api.js";
import type { Run } from "../lib/contracts.js";
import { RunComparison } from "./RunComparison.js";
import { RunDownloads } from "./RunDownloads.js";
import { RunMetrics } from "./RunMetrics.js";
import { RunPlayback } from "./RunPlayback.js";

export function ResultsExplorer({
  api,
  run,
  terminalTick,
}: {
  api: Api;
  run: Run;
  terminalTick: number;
}) {
  const [tab, setTab] = useState("metrics");
  return (
    <section className="panel" aria-label="Persisted results explorer">
      <h2>Explore persisted results</h2>
      <p className="mono">Run {run.id}</p>
      <nav aria-label="Result views">
        {["metrics", "playback", "comparison"].map((name) => (
          <button
            type="button"
            key={name}
            aria-current={tab === name ? "page" : undefined}
            onClick={() => setTab(name)}
          >
            {name[0]?.toUpperCase()}
            {name.slice(1)}
          </button>
        ))}
      </nav>
      {tab === "metrics" && <RunMetrics api={api} runId={run.id} terminalTick={terminalTick} />}
      {tab === "playback" && <RunPlayback api={api} runId={run.id} terminalTick={terminalTick} />}
      {tab === "comparison" && <RunComparison api={api} run={run} />}
      <RunDownloads key={run.id} api={api} runId={run.id} />
    </section>
  );
}

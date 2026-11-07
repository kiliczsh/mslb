import { useState } from "react";
import type { Api } from "../../shared/api/api.js";
import type { Run } from "../../shared/api/contracts.js";
import { ChartLineIcon, DownloadSimpleIcon, PlayIcon, ScalesIcon } from "../../shared/ui/Icons.js";
import { RunComparison } from "./RunComparison.js";
import { RunDownloads } from "./RunDownloads.js";
import { RunMetrics } from "./RunMetrics.js";
import { RunPlayback } from "./RunPlayback.js";

export function ResultsExplorer({
  api,
  run,
  terminalTick,
  autoPlay = false,
  onAutoPlayStarted,
}: {
  api: Api;
  run: Run;
  terminalTick: number;
  autoPlay?: boolean;
  onAutoPlayStarted?: (() => void) | undefined;
}) {
  const [tab, setTab] = useState(autoPlay ? "playback" : "metrics");
  return (
    <section className="panel results-explorer" aria-label="Persisted results explorer">
      <nav aria-label="Result views">
        {[
          { name: "metrics", Icon: ChartLineIcon },
          { name: "playback", Icon: PlayIcon },
          { name: "comparison", Icon: ScalesIcon },
          { name: "downloads", Icon: DownloadSimpleIcon },
        ].map(({ name, Icon }) => (
          <button
            type="button"
            key={name}
            aria-current={tab === name ? "page" : undefined}
            onClick={() => setTab(name)}
          >
            <Icon size={16} aria-hidden="true" />
            {name[0]?.toUpperCase()}
            {name.slice(1)}
          </button>
        ))}
      </nav>
      {tab === "metrics" && <RunMetrics api={api} runId={run.id} terminalTick={terminalTick} />}
      {tab === "playback" && (
        <RunPlayback
          api={api}
          runId={run.id}
          terminalTick={terminalTick}
          autoPlay={autoPlay}
          onAutoPlayStarted={onAutoPlayStarted}
        />
      )}
      {tab === "comparison" && <RunComparison api={api} run={run} />}
      {tab === "downloads" && <RunDownloads key={run.id} api={api} runId={run.id} />}
    </section>
  );
}

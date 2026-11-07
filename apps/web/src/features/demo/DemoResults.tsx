import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import type { Api } from "../../shared/api/api.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import { CaretRightIcon, SkipBackIcon } from "../../shared/ui/Icons.js";
import { FlowTopology } from "../results/FlowTopology.js";
import { MetricNumber } from "../results/MetricNumber.js";
import { DemoLoading } from "./DemoLoading.js";

export function DemoResults({
  api,
  runId,
  terminalTick,
}: {
  api: Api;
  runId: string;
  terminalTick: number;
}) {
  const [tick, setTick] = useState(0);
  const [offset, setOffset] = useState(0);
  const [selected, setSelected] = useState<string | null>(null);
  const metrics = useQuery({
    queryKey: ["metrics", runId],
    queryFn: ({ signal }) => api.metrics(runId, signal),
  });
  const frame = useQuery({
    queryKey: ["demo-playback", runId, tick, offset],
    placeholderData: keepPreviousData,
    queryFn: ({ signal }) =>
      api.playback(
        runId,
        {
          tick,
          server_offset: offset,
          request_offset: 0,
          request_kind: "pending",
          server_id: null,
        },
        signal,
      ),
  });
  if (metrics.isPending && !metrics.error) {
    return (
      <DemoLoading
        title="Loading saved results…"
        detail="Reading metrics and the selected playback tick."
      />
    );
  }
  return (
    <section aria-label="Guided saved results">
      <section className="demo-metrics" aria-label="Whole-run results">
        <div>
          <span className="demo-metric-label">Finished</span>
          <strong>{metrics.data?.summary.finished ?? "—"}</strong>
        </div>
        <div>
          <span className="demo-metric-label">Dropped</span>
          <strong>{metrics.data?.summary.dropped ?? "—"}</strong>
        </div>
        <div>
          <span className="demo-metric-label">Avg response · ticks</span>
          <strong>
            <MetricNumber value={metrics.data?.summary.response.mean ?? null} />
          </strong>
        </div>
      </section>
      <Feedback error={metrics.error ?? frame.error} />
      <div className="demo-playback-control">
        <label>
          Saved tick · {tick} / {terminalTick}
          <input
            type="range"
            min={0}
            max={terminalTick}
            value={tick}
            onChange={(event) => setTick(event.target.valueAsNumber)}
          />
        </label>
        <button type="button" onClick={() => setTick(0)} disabled={tick === 0}>
          <SkipBackIcon size={16} aria-hidden="true" /> First tick
        </button>
        <button
          type="button"
          onClick={() => setTick((value) => Math.min(terminalTick, value + 1))}
          disabled={tick >= terminalTick}
        >
          Next tick <CaretRightIcon size={16} aria-hidden="true" />
        </button>
      </div>
      <p className="muted" role="status">
        {frame.isFetching
          ? `Loading tick ${tick}…${frame.data ? ` Showing tick ${frame.data.tick} until it is ready.` : ""}`
          : frame.data
            ? `Showing saved tick ${frame.data.tick}.`
            : "Saved playback is unavailable."}
      </p>
      {frame.isPending && !frame.error && (
        <DemoLoading
          title={`Loading saved tick ${tick}…`}
          detail="Reading the saved request flow."
        />
      )}
      <div aria-busy={frame.isFetching}>
        {frame.data && (
          <FlowTopology
            frame={frame.data}
            playing={false}
            selectedId={selected}
            onInspect={setSelected}
          />
        )}
      </div>
      {frame.data && frame.data.membership_count > 32 && (
        <div className="actions">
          <button
            type="button"
            disabled={offset === 0}
            onClick={() => setOffset((value) => Math.max(0, value - 32))}
          >
            Previous servers
          </button>
          <button
            type="button"
            disabled={offset + 32 >= frame.data.membership_count}
            onClick={() => setOffset((value) => value + 32)}
          >
            Next servers
          </button>
        </div>
      )}
      <p className="muted">Metrics cover the whole run. The scene shows the selected tick.</p>
    </section>
  );
}

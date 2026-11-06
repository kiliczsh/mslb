import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { z } from "zod";
import type { Api } from "../lib/api.js";
import { chartPoints } from "../lib/chartPoints.js";
import { exactText } from "../lib/metricValues.js";
import {
  type Chart,
  chartMetricSchema,
  type serverMetricSchema,
  type totalsSchema,
} from "../lib/resultContracts.js";
import { Feedback } from "./Feedback.js";
import { MetricNumber } from "./MetricNumber.js";

function Totals({ value }: { value: z.infer<typeof totalsSchema> }) {
  return (
    <>
      <dl className="metric-grid">
        {(
          [
            ["Useful CPU / integrated capacity", value.cpu_utilization],
            ["Reserved memory / integrated capacity", value.memory_utilization],
            ["Allocated CPU work", value.cpu_allocated],
            ["Useful CPU work", value.cpu_consumed],
            ["Integrated CPU capacity (units × ticks)", value.cpu_capacity_ticks],
            ["Reserved memory (MB × ticks)", value.memory_reserved_mb_ticks],
            ["Memory capacity (MB × ticks)", value.memory_capacity_mb_ticks],
          ] as const
        ).map(([name, metric]) => (
          <div key={name}>
            <dt>{name}</dt>
            <dd>
              <MetricNumber value={metric} />
            </dd>
          </div>
        ))}
      </dl>
      <div className="table-scroll">
        <table>
          <caption>Latency distributions in simulated ticks · nearest-rank percentiles</caption>
          <thead>
            <tr>
              <th scope="col">Distribution</th>
              <th scope="col">Samples</th>
              <th scope="col">Mean</th>
              <th scope="col">Minimum</th>
              <th scope="col">Maximum</th>
              <th scope="col">P50</th>
              <th scope="col">P95</th>
            </tr>
          </thead>
          <tbody>
            {(["waiting", "response", "execution"] as const).map((name) => (
              <tr key={name}>
                <th scope="row">{name}</th>
                <td>{value[name].count}</td>
                {(["mean", "min", "max", "p50", "p95"] as const).map((field) => (
                  <td key={field}>
                    <MetricNumber value={value[name][field]} />
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

export function RunMetrics({
  api,
  runId,
  terminalTick,
}: {
  api: Api;
  runId: string;
  terminalTick: number;
}) {
  const [cursor, setCursor] = useState<string | null>(null);
  const [selected, setSelected] = useState<z.infer<typeof serverMetricSchema> | null>(null);
  const [metric, setMetric] = useState<Chart["metric"]>("cpu_utilization");
  const [start, setStart] = useState(0);
  const [end, setEnd] = useState(Math.max(1, terminalTick));
  const [showExact, setShowExact] = useState(false);
  const report = useQuery({
    queryKey: ["metrics", runId],
    queryFn: ({ signal }) => api.metrics(runId, signal),
  });
  const servers = useQuery({
    queryKey: ["metric-servers", runId, cursor],
    queryFn: ({ signal }) => api.serverMetrics(runId, cursor, signal),
  });
  const valid =
    Number.isInteger(start) &&
    Number.isInteger(end) &&
    start >= 0 &&
    start < end &&
    end <= 1_000_000;
  const serverId = metric === "pending_depth" ? null : (selected?.id ?? null);
  const chart = useQuery({
    queryKey: ["chart", runId, metric, start, end, serverId],
    queryFn: ({ signal }) => api.chart(runId, metric, start, end, serverId, signal),
    enabled: valid,
  });
  const plotted = chartPoints(chart.data?.points ?? []);
  return (
    <section aria-label="Completed metrics">
      <h3>Exact completed metrics</h3>
      <Feedback error={report.error} />
      {report.isPending && <p role="status">Loading saved metrics…</p>}
      {report.data && (
        <>
          <dl className="metric-grid">
            {(
              [
                ["Arrived", report.data.summary.arrived],
                ["Started", report.data.summary.started],
                ["Finished", report.data.summary.finished],
                ["Dropped", report.data.summary.dropped],
                ["Makespan (ticks from earliest arrival)", report.data.summary.makespan],
                [
                  "Completed throughput (requests/second)",
                  report.data.summary.completed_throughput,
                ],
                ["Whole-run drop fraction", report.data.summary.drop_fraction],
                ["Queue peak", report.data.summary.queue_depth_peak],
                ["Queue integral (requests × ticks)", report.data.summary.queue_depth_integral],
              ] as const
            ).map(([label, value]) => (
              <div key={label}>
                <dt>{label}</dt>
                <dd>
                  <MetricNumber value={value} />
                </dd>
              </div>
            ))}
          </dl>
          <h4>
            {selected
              ? `Server ${selected.id}${selected.metrics.active ? "" : " · removed before completion"}`
              : "Cluster totals"}
          </h4>
          <Totals value={selected?.metrics ?? report.data.summary.cluster} />
          <details>
            <summary>Definitions and exact values</summary>
            <p>
              Wait = start − arrival, response = finish − arrival, execution = finish − start.
              Successful latency samples exclude dropped requests; undefined distributions stay
              undefined. Utilization integrates [0, terminal tick), including initial idle time and
              changing membership. Logical MB is not runtime RSS.
            </p>
            <p>
              Allocated CPU may exceed useful consumption when a request finishes with less work
              than its share. Approximate decimal labels begin with ≈. Every number exposes its
              exact value in its title; the saved summary below remains authoritative.
            </p>
            <pre>{JSON.stringify(selected?.metrics ?? report.data.summary, null, 2)}</pre>
          </details>
        </>
      )}
      <details>
        <summary>Browse all historical metric servers</summary>
        <Feedback error={servers.error} />
        <div className="actions">
          <button type="button" onClick={() => setSelected(null)}>
            Use cluster totals
          </button>
          {servers.data?.items.map((server) => (
            <button
              type="button"
              key={server.id}
              aria-pressed={selected?.id === server.id}
              onClick={() => setSelected(server)}
            >
              {server.id}
            </button>
          ))}
        </div>
        <div className="actions">
          <button type="button" disabled={cursor === null} onClick={() => setCursor(null)}>
            First metric servers
          </button>
          <button
            type="button"
            disabled={!servers.data?.next_cursor}
            onClick={() => setCursor(servers.data?.next_cursor ?? null)}
          >
            Next metric servers
          </button>
        </div>
      </details>
      <h4>Persisted interval chart · {serverId ?? "cluster"}</h4>
      <div className="form-grid">
        <label>
          Chart metric
          <select
            value={metric}
            onChange={(event) => setMetric(chartMetricSchema.parse(event.target.value))}
          >
            <option value="cpu_utilization">Useful CPU utilization</option>
            <option value="memory_utilization">Logical memory utilization</option>
            <option value="pending_depth">Cluster queue depth</option>
            <option value="active_requests">Active request count</option>
          </select>
        </label>
        <label>
          Range start (inclusive)
          <input
            type="number"
            min={0}
            max={999_999}
            step={1}
            value={Number.isFinite(start) ? start : ""}
            onChange={(event) => setStart(event.target.valueAsNumber)}
          />
        </label>
        <label>
          Range end (exclusive)
          <input
            type="number"
            min={1}
            max={1_000_000}
            step={1}
            value={Number.isFinite(end) ? end : ""}
            onChange={(event) => setEnd(event.target.valueAsNumber)}
          />
        </label>
      </div>
      {!valid && <p role="alert">Choose an increasing whole-tick range inside 0–1,000,000.</p>}
      <Feedback error={chart.error} />
      {chart.isFetching && <p role="status">Reading bounded chart buckets…</p>}
      <p>
        At most 500 buckets. Minimum, maximum, and time-weighted mean cover observed intervals only.
        Missing membership produces gaps, not zeros. Queue depth is always cluster-wide.
      </p>
      {chart.data &&
        (plotted.length ? (
          <section className="chart-frame" aria-label="Interval metric chart">
            <ResponsiveContainer width="100%" height={300} minWidth={1}>
              <LineChart data={plotted} accessibilityLayer>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="t" type="number" domain={[start, end]} />
                <YAxis width={70} />
                <Tooltip />
                <Legend />
                <Line
                  name="Minimum"
                  type="stepAfter"
                  dataKey="minimum"
                  stroke="#777f93"
                  dot={plotted.length <= 32}
                  isAnimationActive={false}
                  connectNulls={false}
                />
                <Line
                  name="Maximum"
                  type="stepAfter"
                  dataKey="maximum"
                  stroke="#b15f18"
                  dot={plotted.length <= 32}
                  isAnimationActive={false}
                  connectNulls={false}
                />
                <Line
                  name="Weighted mean"
                  type="stepAfter"
                  dataKey="mean"
                  stroke="#176555"
                  dot={plotted.length <= 32}
                  isAnimationActive={false}
                  connectNulls={false}
                />
              </LineChart>
            </ResponsiveContainer>
          </section>
        ) : (
          <p>No covered resource intervals in this range.</p>
        ))}
      <button type="button" onClick={() => setShowExact(!showExact)} aria-expanded={showExact}>
        Inspect exact chart buckets
      </button>
      {showExact && (
        <div className="table-scroll">
          <table>
            <caption>Exact chart buckets · {chart.data?.points.length ?? 0} points</caption>
            <thead>
              <tr>
                <th scope="col">Range</th>
                <th scope="col">Covered ticks</th>
                <th scope="col">Minimum</th>
                <th scope="col">Maximum</th>
                <th scope="col">Mean</th>
              </tr>
            </thead>
            <tbody>
              {chart.data?.points.map((point) => (
                <tr key={point.t}>
                  <th scope="row">
                    [{point.t}, {point.end_tick})
                  </th>
                  <td>{point.covered_ticks}</td>
                  <td>{exactText(point.min)}</td>
                  <td>{exactText(point.max)}</td>
                  <td>{exactText(point.mean)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

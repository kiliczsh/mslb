import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import type { Api } from "../../shared/api/api.js";
import { terminal } from "../../shared/api/contracts.js";
import type { DockerDetail as Detail } from "../../shared/api/dockerContracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import { StatusIcon } from "../../shared/ui/StatusIcon.js";
import { failureMessage } from "../history/historyContracts.js";
import { RunDownloads } from "../results/RunDownloads.js";
import { DockerMeasurements } from "./DockerMeasurements.js";
import { ScrollTable } from "./ScrollableTable.js";

function Events({ api, id }: { api: Api; id: string }) {
  const [cursor, setCursor] = useState<string | null>(null);
  const page = useQuery({
    queryKey: ["docker-events", id, cursor],
    queryFn: ({ signal }) => api.dockerEvents({ id, cursor, signal }),
  });
  return (
    <section aria-label="Actual HTTP event history">
      <h3>Actual HTTP events</h3>
      <Feedback error={page.error} />
      <ScrollTable label="HTTP event table">
        <table>
          <caption>At most 100 saved observations per page. Times are elapsed seconds.</caption>
          <thead>
            <tr>
              {["Sequence", "Seconds", "Request", "Outcome", "Backend", "Work (ms)", "Reason"].map(
                (label) => (
                  <th key={label} scope="col">
                    {label}
                  </th>
                ),
              )}
            </tr>
          </thead>
          <tbody>
            {page.data?.items.map((event) => (
              <tr key={event.sequence}>
                <td>{event.sequence}</td>
                <td>{event.elapsed_seconds.toFixed(3)}</td>
                <td>{event.request_id}</td>
                <td>{event.kind}</td>
                <td>{event.backend_id ?? "—"}</td>
                <td>{event.result?.duration_ms.toFixed(1) ?? "—"}</td>
                <td>{event.reason ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </ScrollTable>
      <div className="actions">
        <button type="button" disabled={!cursor} onClick={() => setCursor(null)}>
          First HTTP events
        </button>
        <button
          type="button"
          disabled={!page.data?.next_cursor}
          onClick={() => setCursor(page.data?.next_cursor ?? null)}
        >
          Next HTTP events
        </button>
      </div>
    </section>
  );
}
function Overview({ value }: { value: Detail }) {
  const summary = value.summary;
  const progress = value.progress;
  const stats = summary
    ? [
        ["Completed", summary.completed],
        ["Failed", summary.failed],
        ["Dropped", summary.dropped],
        ["Unexecuted", summary.unexecuted],
        ["Elapsed", `${summary.elapsed_seconds.toFixed(1)}s`],
        [
          "Throughput",
          summary.elapsed_seconds > 0
            ? `${(summary.completed / summary.elapsed_seconds).toFixed(1)} req/s`
            : "—",
        ],
      ]
    : [
        ["Active HTTP", progress?.inflight ?? "—"],
        ["Pending queue", progress?.pending ?? "—"],
        [
          "Ready replicas",
          progress ? progress.backends.filter((sample) => sample.health.ready).length : "—",
        ],
        ["Elapsed", progress ? `${progress.elapsed_seconds.toFixed(1)}s` : "—"],
      ];
  return (
    <section aria-label="HTTP overview">
      <dl className="docker-overview-metrics">
        {stats.map(([label, count]) => (
          <div key={label}>
            <dt>{label}</dt>
            <dd>{count}</dd>
          </div>
        ))}
      </dl>
      {summary ? (
        <p className="muted">
          {summary.total.toLocaleString()} input requests · peak queue {summary.peak_pending} · peak
          HTTP reservations {summary.peak_inflight}. Per-request backend work durations are
          available in HTTP events.
        </p>
      ) : (
        <p className="muted">
          Live values reflect the latest saved observation. Final completed, failed and dropped
          counts appear after verification.
        </p>
      )}
      {!summary && progress && (
        <p className="muted">
          Last observation:{" "}
          <time dateTime={progress.observed_at}>
            {new Date(progress.observed_at).toLocaleTimeString()}
          </time>
        </p>
      )}
      {!summary && !progress && <p role="status">Waiting for the first HTTP observation…</p>}
      {value.cleanup_pending &&
        (terminal(value.run.status) || value.run.status === "cancel_requested") && (
          <p className="notice" role="status">
            Traffic has ended or is stopping. Owned resources are still being cleaned up.
          </p>
        )}
    </section>
  );
}
function Resources({ value }: { value: Detail }) {
  return (
    <section aria-label="Replica resources">
      <DockerMeasurements value={value} active={!terminal(value.run.status)} />
      <h3>Backend lifecycle</h3>
      <ScrollTable label="Backend lifecycle table">
        <table>
          <thead>
            <tr>
              <th scope="col">Backend</th>
              <th scope="col">Kind</th>
              <th scope="col">State</th>
            </tr>
          </thead>
          <tbody>
            {value.resources.map((resource) => (
              <tr key={resource.id}>
                <th scope="row">{resource.name}</th>
                <td>{resource.kind}</td>
                <td>{resource.state}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </ScrollTable>
      {value.resources.length === 0 && <p>No backend resources reserved yet.</p>}
    </section>
  );
}
export function DockerDetail({ api, id }: { api: Api; id: string }) {
  const [tab, setTab] = useState("overview");
  const client = useQueryClient();
  const detail = useQuery({
    queryKey: ["docker-run", id],
    queryFn: ({ signal }) => api.dockerRun({ id, signal }),
    refetchInterval: (query) =>
      query.state.data && terminal(query.state.data.run.status) && !query.state.data.cleanup_pending
        ? false
        : 1000,
  });
  const cancel = useMutation({
    mutationFn: () => api.dockerCancel({ id }),
    onSuccess: async () => {
      await client.invalidateQueries({ queryKey: ["docker-run", id] });
      await client.invalidateQueries({ queryKey: ["browser-history", "docker"] });
    },
  });
  const value = detail.data;
  return (
    <section className="docker-detail" aria-label="Selected Docker run">
      <h2>Traffic run</h2>
      <Feedback error={detail.error ?? cancel.error} />
      {detail.isPending && <p role="status">Loading traffic run…</p>}
      {value && (
        <>
          <div className="docker-run-status">
            <span className="icon-label" role="status">
              <StatusIcon status={value.run.status} />
              {value.cleanup_pending && terminal(value.run.status)
                ? "Cleaning up resources"
                : value.run.status.replaceAll("_", " ")}
            </span>
            <span className="mono" title={id}>
              {id.slice(0, 8)}
            </span>
            <span>{value.run.configuration.strategy.replaceAll("_", " ")}</span>
            {!terminal(value.run.status) && (
              <button
                type="button"
                disabled={cancel.isPending || value.run.status === "cancel_requested"}
                onClick={() => cancel.mutate()}
              >
                {cancel.isPending || value.run.status === "cancel_requested"
                  ? "Stopping…"
                  : "Stop traffic"}
              </button>
            )}
          </div>
          {value.run.error_code && (
            <p className="notice error" role="alert">
              {failureMessage(value.run.error_code)} <small>{value.run.error_code}</small>
            </p>
          )}
          <nav className="view-tabs" aria-label="Traffic result views">
            {["overview", "resources", "events", "downloads"].map((name) => (
              <button
                key={name}
                type="button"
                aria-pressed={tab === name}
                disabled={(name === "events" || name === "downloads") && !value.summary}
                onClick={() => setTab(name)}
              >
                {
                  (
                    {
                      overview: "Overview",
                      resources: "CPU / Memory / Replicas",
                      events: "HTTP events",
                      downloads: "Downloads",
                    } as Record<string, string>
                  )[name]
                }
              </button>
            ))}
          </nav>
          {tab === "overview" && <Overview value={value} />}
          {tab === "resources" && <Resources value={value} />}
          {tab === "events" && value.summary && <Events api={api} id={id} />}
          {tab === "downloads" && value.summary && (
            <RunDownloads api={api} runId={id} mode="docker" />
          )}
          <details className="docker-saved-settings">
            <summary>Saved settings and diagnostics</summary>
            <p className="mono">Run: {id}</p>
            <p className="mono">Snapshot: {value.run.snapshot_digest}</p>
            <pre>{JSON.stringify(value.run.configuration, null, 2)}</pre>
            {value.admission && (
              <p>
                Resource admission:{" "}
                {value.admission.admit ? "allowed" : `held · ${value.admission.reason}`}
                {value.admission.stop_running ? " · running work must stop" : ""}.
              </p>
            )}
          </details>
        </>
      )}
    </section>
  );
}

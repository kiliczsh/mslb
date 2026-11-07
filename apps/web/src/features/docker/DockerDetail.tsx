import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import type { Api } from "../../shared/api/api.js";
import { terminal } from "../../shared/api/contracts.js";
import type { DockerDetail as Detail } from "../../shared/api/dockerContracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";
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
              {["Sequence", "Seconds", "Request", "Outcome", "Backend", "Reason"].map((label) => (
                <th key={label} scope="col">
                  {label}
                </th>
              ))}
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
function Result({ api, value }: { api: Api; value: Detail }) {
  const summary = value.summary;
  return (
    <>
      {summary && (
        <section aria-label="Real HTTP results">
          <h3>Real HTTP results</h3>
          <dl className="metric-grid">
            {(
              [
                ["Total requests", summary.total],
                ["Completed HTTP requests", summary.completed],
                ["Failed HTTP requests", summary.failed],
                ["Dropped requests", summary.dropped],
                ["Unexecuted requests", summary.unexecuted],
                ["Elapsed seconds", summary.elapsed_seconds.toFixed(3)],
                ["Peak pending requests", summary.peak_pending],
                ["Peak HTTP reservations", summary.peak_inflight],
              ] as const
            ).map(([label, number]) => (
              <div key={label}>
                <dt>{label}</dt>
                <dd>{number}</dd>
              </div>
            ))}
          </dl>
          <p>
            Completion rate:{" "}
            {summary.elapsed_seconds > 0
              ? (summary.completed / summary.elapsed_seconds).toFixed(3)
              : "unavailable"}{" "}
            requests/second. These operational outcomes are independently checked against the
            matching HTTP and lifecycle files.
          </p>
          <DockerMeasurements value={value} active={false} />
          <RunDownloads api={api} runId={value.run.id} mode="docker" />
          <Events api={api} id={value.run.id} />
        </section>
      )}
      <section aria-label="Owned backend lifecycle">
        <h3>Backend lifecycle</h3>
        <ScrollTable label="Backend lifecycle table">
          <table>
            <caption>Durable owned resources · at most 64 lifecycle identities</caption>
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
    </>
  );
}
export function DockerDetail({ api, id }: { api: Api; id: string }) {
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
      await client.invalidateQueries({ queryKey: ["docker-runs"] });
    },
  });
  const value = detail.data;
  return (
    <section aria-label="Selected Docker run">
      <h2>Docker run</h2>
      <p className="digest">{id}</p>
      <Feedback error={detail.error ?? cancel.error} />
      {value && (
        <>
          <p role="status">
            Docker status: {value.run.status}
            {value.cleanup_pending ? " · cleanup pending" : ""}
          </p>
          {value.run.error_code && <p role="alert">Run diagnostic: {value.run.error_code}</p>}
          {!terminal(value.run.status) && (
            <button
              type="button"
              disabled={cancel.isPending || value.run.status === "cancel_requested"}
              onClick={() => cancel.mutate()}
            >
              Cancel Docker run
            </button>
          )}
          <p>
            Snapshot: <span className="digest">{value.run.snapshot_digest}</span>
          </p>
          {value.admission && (
            <p>
              Growth admission when this run was last read:{" "}
              {value.admission.admit ? "allowed" : `held · ${value.admission.reason}`}.
              {value.admission.stop_running
                ? " Current resource evidence requires running work to stop."
                : ""}
            </p>
          )}
          {!terminal(value.run.status) && <DockerMeasurements value={value} active />}
          {!value.summary && (
            <p>
              Verified result files become available after successful completion and owned cleanup.
            </p>
          )}
          <Result key={id} api={api} value={value} />
        </>
      )}
    </section>
  );
}

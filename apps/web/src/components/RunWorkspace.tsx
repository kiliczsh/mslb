import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { lazy, Suspense, useRef, useState } from "react";
import { type Api, idempotencyKey } from "../lib/api.js";
import {
  type Dataset,
  defaultSettings,
  type Settings,
  settingsSchema,
  terminal,
} from "../lib/contracts.js";
import { scalingPolicyError } from "../lib/scalingPolicy.js";
import { AutoscalingControls } from "./AutoscalingControls.js";
import { Feedback } from "./Feedback.js";
import { LiveRun } from "./LiveRun.js";

const ResultsExplorer = lazy(() =>
  import("./ResultsExplorer.js").then((module) => ({ default: module.ResultsExplorer })),
);

export function RunWorkspace({ api, dataset }: { api: Api; dataset: Dataset | null }) {
  const client = useQueryClient();
  const [cursor, setCursor] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [settings, setSettings] = useState<Settings>(structuredClone(defaultSettings));
  const [localError, setLocalError] = useState("");
  const [reviewKey, setReviewKey] = useState<string | null>(null);
  const retry = useRef<{ body: string; key: string } | null>(null);
  const servers = useQuery({ queryKey: ["servers"], queryFn: ({ signal }) => api.servers(signal) });
  const currentReviewKey = JSON.stringify({ dataset, servers: servers.data?.items, settings });
  const reviewed = reviewKey === currentReviewKey;
  const history = useQuery({
    queryKey: ["runs", cursor],
    queryFn: ({ signal }) => api.runs(cursor, signal),
    refetchInterval: 3000,
  });
  const detail = useQuery({
    queryKey: ["run", selected],
    queryFn: ({ signal }) => api.run(selected ?? "", signal),
    enabled: selected !== null,
    refetchInterval: (query) =>
      query.state.data && terminal(query.state.data.status) ? false : 1500,
  });
  const launch = useMutation({
    mutationFn: ({ datasetId, configuration }: { datasetId: string; configuration: Settings }) => {
      const body = JSON.stringify({ datasetId, configuration });
      if (retry.current?.body !== body) retry.current = { body, key: idempotencyKey() };
      return api.submit(datasetId, configuration, retry.current.key);
    },
    onSuccess: async (run) => {
      setSelected(run.run_id);
      setCursor(null);
      setReviewKey(null);
      retry.current = null;
      await client.invalidateQueries({ queryKey: ["runs"] });
    },
  });
  const cancel = useMutation({
    mutationFn: (id: string) => api.cancel(id),
    onSuccess: async () => {
      await client.invalidateQueries({ queryKey: ["run", selected] });
      await client.invalidateQueries({ queryKey: ["runs"] });
    },
  });
  function change(next: Settings) {
    setSettings(next);
    setReviewKey(null);
    setLocalError("");
    launch.reset();
  }
  function validated() {
    const result = settingsSchema.safeParse(settings);
    if (result.success) {
      const policyError = scalingPolicyError({
        policy: result.data.autoscaling,
        initialServerCount: servers.data?.items.length ?? 0,
      });
      setLocalError(policyError ?? "");
      return policyError ? null : result.data;
    }
    setLocalError(
      result.error.issues
        .map((issue) => `${issue.path.join(".")}: ${issue.message}`)
        .slice(0, 8)
        .join("; "),
    );
    return null;
  }
  return (
    <section aria-labelledby="runs-heading">
      <h2 id="runs-heading">Simulations</h2>
      <p>
        Logical time is deterministic. Worker recovery and wall-clock progress are operational
        state.
      </p>
      <form
        className="panel"
        onSubmit={(event) => {
          event.preventDefault();
          const configuration = validated();
          if (!configuration || !dataset || !servers.data?.items.length) return;
          if (!reviewed) {
            setReviewKey(currentReviewKey);
            return;
          }
          launch.mutate({ datasetId: dataset.id, configuration });
        }}
      >
        <h3>Configure a logical run</h3>
        <p>
          {dataset ? (
            <>
              Selected dataset: <strong>{dataset.source_filename}</strong> · {dataset.request_count}{" "}
              requests
            </>
          ) : (
            "Select a dataset in Datasets before launching."
          )}
        </p>
        <div className="form-grid">
          <label>
            Execution mode
            <select
              value={settings.mode}
              onChange={(event) =>
                change({ ...settings, mode: settingsSchema.shape.mode.parse(event.target.value) })
              }
            >
              <option value="concurrent">Concurrent · shared CPU</option>
              <option value="sequential_compat">Sequential · original validator compatible</option>
            </select>
          </label>
          <label>
            Scheduling strategy
            <select
              value={settings.strategy}
              onChange={(event) =>
                change({
                  ...settings,
                  strategy: settingsSchema.shape.strategy.parse(event.target.value),
                })
              }
            >
              <option value="first_fit">First Fit</option>
              <option value="round_robin">Round Robin</option>
              <option value="least_connections">Least Connections</option>
              <option value="most_available_memory">Most Available Memory</option>
            </select>
          </label>
          <label>
            Queue capacity (blank: disabled)
            <input
              type="number"
              min={0}
              max={10_000}
              step={1}
              value={
                settings.max_queue_size !== null && Number.isFinite(settings.max_queue_size)
                  ? settings.max_queue_size
                  : ""
              }
              onChange={(event) =>
                change({
                  ...settings,
                  max_queue_size: event.target.value === "" ? null : event.target.valueAsNumber,
                })
              }
            />
          </label>
          <label>
            Queue timeout ticks (blank: disabled)
            <input
              type="number"
              min={1}
              max={1_000_000}
              step={1}
              value={
                settings.queue_timeout_ticks !== null &&
                Number.isFinite(settings.queue_timeout_ticks)
                  ? settings.queue_timeout_ticks
                  : ""
              }
              onChange={(event) =>
                change({
                  ...settings,
                  queue_timeout_ticks:
                    event.target.value === "" ? null : event.target.valueAsNumber,
                })
              }
            />
          </label>
          <label>
            Simulation horizon (ticks)
            <input
              type="number"
              required
              min={1}
              max={1_000_000}
              step={1}
              value={
                Number.isFinite(settings.max_simulation_ticks) ? settings.max_simulation_ticks : ""
              }
              onChange={(event) =>
                change({ ...settings, max_simulation_ticks: event.target.valueAsNumber })
              }
            />
          </label>
        </div>
        <AutoscalingControls
          policy={settings.autoscaling}
          onChange={(autoscaling) => change({ ...settings, autoscaling })}
        />
        <details>
          <summary>Effective execution limits</summary>
          <p>
            At most 64 logical servers, 10,000 input requests, 10 MiB CSV. Limits can be lowered;
            domain admission may reject a configuration that exceeds the installation envelope.
          </p>
          <div className="form-grid">
            {(["max_samples", "max_fraction_bits", "max_metric_series"] as const).map((key) => (
              <label key={key}>
                {
                  {
                    max_samples: "Maximum samples",
                    max_fraction_bits: "Maximum exact fraction bits",
                    max_metric_series: "Maximum metric histories",
                  }[key]
                }
                <input
                  type="number"
                  required
                  min={1}
                  max={
                    { max_samples: 1_000_000, max_fraction_bits: 4096, max_metric_series: 10_000 }[
                      key
                    ]
                  }
                  step={1}
                  value={Number.isFinite(settings[key]) ? settings[key] : ""}
                  onChange={(event) => change({ ...settings, [key]: event.target.valueAsNumber })}
                />
              </label>
            ))}
          </div>
        </details>
        <Feedback error={servers.error} />
        <Feedback error={launch.error} />
        {localError && <p role="alert">{localError}</p>}
        {launch.isError && (
          <p className="notice">
            A retry with unchanged settings reuses the same idempotency key. Check history if the
            previous response was interrupted.
          </p>
        )}
        {reviewed && (
          <div className="notice" role="status">
            <h3>Review snapshot inputs</h3>
            <p>
              Dataset {dataset?.id}. The saved snapshot returned after submission is authoritative;
              current servers can change before registration.
            </p>
            <ul>
              {servers.data?.items.map((server) => (
                <li key={server.id}>
                  {server.id} · CPU {server.cpu_units_per_tick} · memory {server.mem_mb} MB · starts{" "}
                  {server.rate_limit_per_sec}/tick · version {server.version}
                </li>
              ))}
            </ul>
            <details>
              <summary>Configuration to submit</summary>
              <pre>{JSON.stringify(settings, null, 2)}</pre>
            </details>
          </div>
        )}
        <button
          type="submit"
          className="primary"
          disabled={!dataset || !servers.data?.items.length || launch.isPending}
        >
          {launch.isPending
            ? "Registering run…"
            : reviewed
              ? "Confirm and queue run"
              : "Review snapshot"}
        </button>
      </form>
      <div className="workspace-grid">
        <div>
          <h3>Run history</h3>
          <Feedback error={history.error} />
          {history.isPending && <p role="status">Loading run history…</p>}
          {history.data?.items.length === 0 && <p>No simulations yet.</p>}
          <ul className="history">
            {history.data?.items.map((run) => (
              <li key={run.id}>
                <button
                  type="button"
                  aria-pressed={run.id === selected}
                  onClick={() => {
                    setSelected(run.id);
                    cancel.reset();
                  }}
                >
                  <strong>{run.configuration.strategy.replaceAll("_", " ")}</strong>
                  <span>{run.status.replaceAll("_", " ")}</span>
                  <small>{run.id}</small>
                  <small>{new Date(run.created_at).toLocaleString()}</small>
                </button>
              </li>
            ))}
          </ul>
          <div className="actions">
            <button type="button" disabled={cursor === null} onClick={() => setCursor(null)}>
              First runs
            </button>
            <button
              type="button"
              disabled={!history.data?.next_cursor}
              onClick={() => setCursor(history.data?.next_cursor ?? null)}
            >
              Next runs
            </button>
          </div>
        </div>
        <div aria-live="polite">
          <h3>Run details</h3>
          <Feedback error={detail.error} />
          {!selected && <p>Select a run to inspect its persisted status.</p>}
          {detail.isFetching && !detail.data && selected && <p>Loading run details…</p>}
          {detail.data && (
            <div className="panel">
              <p className="mono">{detail.data.id}</p>
              <p className="status">{detail.data.status.replaceAll("_", " ")}</p>
              <p>
                {detail.data.mode} · {detail.data.strategy} · attempt {detail.data.attempts}
              </p>
              {detail.data.progress && (
                <p>
                  Committed through tick {detail.data.progress.next_tick};{" "}
                  {detail.data.progress.committed_events} events saved.
                </p>
              )}
              {detail.data.error_code && (
                <p role="alert">Run ended with {detail.data.error_code}.</p>
              )}
              {!terminal(detail.data.status) && (
                <button
                  type="button"
                  disabled={cancel.isPending || detail.data.status === "cancel_requested"}
                  onClick={() => cancel.mutate(detail.data.id)}
                >
                  {detail.data.status === "cancel_requested"
                    ? "Cancellation pending"
                    : "Cancel simulation"}
                </button>
              )}
              <Feedback error={cancel.error} />
              {detail.data.validation && (
                <>
                  <p>
                    Independent replay: {detail.data.validation.independent_replay}. Original
                    validator: {detail.data.validation.original_validator.replaceAll("_", " ")}.
                  </p>
                  <p>
                    Terminal tick {detail.data.validation.terminal_tick} ·{" "}
                    {detail.data.validation.event_count} events
                  </p>
                </>
              )}
              <details>
                <summary>Reproducibility and saved configuration</summary>
                <p className="mono">Snapshot: {detail.data.snapshot_digest}</p>
                <p className="mono">Dataset: {detail.data.dataset_digest}</p>
                <p className="mono">Server snapshot: {detail.data.server_snapshot_digest}</p>
                <pre>{JSON.stringify(detail.data.configuration, null, 2)}</pre>
              </details>
              <button
                type="button"
                onClick={() => {
                  change(structuredClone(detail.data.configuration));
                }}
              >
                Copy settings for a new run
              </button>
            </div>
          )}
        </div>
      </div>
      {detail.data && <LiveRun key={detail.data.id} api={api} run={detail.data} />}
      {detail.data?.validation && (
        <Suspense fallback={<p role="status">Loading result views…</p>}>
          <ResultsExplorer
            key={detail.data.id}
            api={api}
            run={detail.data}
            terminalTick={detail.data.validation.terminal_tick}
          />
        </Suspense>
      )}
    </section>
  );
}

import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { lazy, Suspense, useState } from "react";
import { useDraftState, useDraftStorage } from "../../app/workspaceDrafts.js";
import { type Api, idempotencyKey } from "../../shared/api/api.js";
import {
  type Dataset,
  type Settings,
  settingsSchema,
  terminal,
} from "../../shared/api/contracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import { Help } from "../../shared/ui/Help.js";
import {
  ArrowRightIcon,
  CircleNotchIcon,
  ClockCounterClockwiseIcon,
  CopyIcon,
  DatabaseIcon,
  EyeIcon,
  FlaskIcon,
  HardDrivesIcon,
  SlidersHorizontalIcon,
} from "../../shared/ui/Icons.js";
import { StatusIcon } from "../../shared/ui/StatusIcon.js";
import { AutoscalingControls } from "./AutoscalingControls.js";
import { LiveRun } from "./LiveRun.js";
import { scalingPolicyError } from "./scalingPolicy.js";

const ResultsExplorer = lazy(() =>
  import("../results/ResultsExplorer.js").then((module) => ({ default: module.ResultsExplorer })),
);

export function RunWorkspace({
  api,
  dataset,
  onChooseDataset,
}: {
  api: Api;
  dataset: Dataset | null;
  onChooseDataset?: () => void;
}) {
  const client = useQueryClient();
  const storage = useDraftStorage();
  const [cursor, setCursor] = useDraftState({ storage, name: "runCursor" });
  const [selected, setSelected] = useDraftState({ storage, name: "selectedRun" });
  const [autoPlaybackRun, setAutoPlaybackRun] = useDraftState({ storage, name: "autoPlaybackRun" });
  const [view, setView] = useState<"configure" | "history" | "results">(
    selected ? "results" : "configure",
  );
  const [settings, setSettings] = useDraftState({ storage, name: "settings" });
  const registering = useIsMutating({ mutationKey: ["launch"] }) > 0;
  const [showEvents, setShowEvents] = useState(false);
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [localError, setLocalError] = useState("");
  const [reviewKey, setReviewKey] = useState<string | null>(null);
  const servers = useQuery({
    enabled: view === "configure",
    queryKey: ["servers"],
    queryFn: ({ signal }) => api.servers(signal),
  });
  const currentReviewKey = JSON.stringify({ dataset, servers: servers.data?.items, settings });
  const reviewed = reviewKey === currentReviewKey;
  const history = useQuery({
    enabled: view === "history",
    queryKey: ["runs", cursor],
    queryFn: ({ signal }) => api.runs(cursor, signal),
    refetchInterval: 3000,
  });
  const detail = useQuery({
    queryKey: ["run", selected],
    queryFn: ({ signal }) => api.run(selected ?? "", signal),
    enabled: selected !== null && view === "results",
    refetchInterval: (query) =>
      query.state.data && terminal(query.state.data.status) ? false : 1500,
  });
  const launch = useMutation({
    mutationKey: ["launch"],
    mutationFn: ({ datasetId, configuration }: { datasetId: string; configuration: Settings }) => {
      const body = JSON.stringify({ datasetId, configuration });
      if (storage.current.retry?.body !== body)
        storage.current.retry = { body, key: idempotencyKey() };
      return api.submit(datasetId, configuration, storage.current.retry.key);
    },
    onSuccess: async (run) => {
      setSelected(run.run_id);
      setAutoPlaybackRun(run.run_id);
      setView("results");
      setCursor(null);
      setReviewKey(null);
      storage.current.retry = null;
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
    if (registering) return;
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
      if (policyError) setAdvancedOpen(true);
      return policyError ? null : result.data;
    }
    setAdvancedOpen(true);
    setLocalError(
      result.error.issues
        .map((issue) => `${issue.path.join(".")}: ${issue.message}`)
        .slice(0, 8)
        .join("; "),
    );
    return null;
  }
  return (
    <section aria-labelledby="runs-heading" className="simulation-workspace">
      <nav className="view-tabs" aria-label="Simulation views">
        <button
          type="button"
          aria-pressed={view === "configure"}
          onClick={() => setView("configure")}
        >
          <FlaskIcon size={16} aria-hidden="true" /> New simulation
        </button>
        <button type="button" aria-pressed={view === "history"} onClick={() => setView("history")}>
          <ClockCounterClockwiseIcon size={16} aria-hidden="true" /> Run history
        </button>
        <button
          type="button"
          disabled={!selected}
          aria-pressed={view === "results"}
          onClick={() => setView("results")}
        >
          <EyeIcon size={16} aria-hidden="true" /> Selected results
        </button>
      </nav>
      {view === "configure" && (
        <>
          <div className="simulation-intro">
            <h2 id="runs-heading">New simulation</h2>
            <p className="muted">
              Run your CSV on the current cluster, then inspect the saved results.
            </p>
          </div>
          <ol className="stepper simulation-steps" aria-label="Simulation setup">
            <li aria-current={!reviewed && !registering ? "step" : undefined}>
              <span>1</span>Configure
            </li>
            <li aria-current={reviewed && !registering ? "step" : undefined}>
              <span>2</span>Review
            </li>
            <li aria-current={registering ? "step" : undefined}>
              <span>3</span>Queue run
            </li>
          </ol>
          <div className="simulation-setup">
            <form
              className="run-form"
              noValidate
              onSubmit={(event) => {
                event.preventDefault();
                if (registering) return;
                const configuration = validated();
                if (!configuration || !dataset || !servers.data?.items.length) return;
                if (!reviewed) {
                  setReviewKey(currentReviewKey);
                  return;
                }
                launch.mutate({ datasetId: dataset.id, configuration });
              }}
            >
              <fieldset className="configuration-fields" disabled={registering}>
                <legend className="sr-only">Configure a logical run</legend>
                <h3 className="simulation-section-title">
                  <DatabaseIcon size={18} aria-hidden="true" /> Dataset
                </h3>
                <div className="simulation-dataset">
                  <p>
                    {dataset ? (
                      <>
                        Selected dataset: <strong>{dataset.source_filename}</strong> ·{" "}
                        {dataset.request_count.toLocaleString()} requests
                      </>
                    ) : (
                      "Select a dataset in Datasets before launching."
                    )}
                  </p>
                  {onChooseDataset && (
                    <button type="button" className="text-button" onClick={onChooseDataset}>
                      {dataset ? "Change dataset" : "Choose dataset"}{" "}
                      <ArrowRightIcon size={16} aria-hidden="true" />
                    </button>
                  )}
                </div>
                <h3 className="simulation-section-title">
                  <SlidersHorizontalIcon size={18} aria-hidden="true" /> Strategy &amp; mode
                </h3>
                <div className="form-grid simulation-basics">
                  <label>
                    Execution mode
                    <select
                      aria-label="Execution mode"
                      aria-describedby="simulation-mode-help"
                      value={settings.mode}
                      onChange={(event) =>
                        change({
                          ...settings,
                          mode: settingsSchema.shape.mode.parse(event.target.value),
                        })
                      }
                    >
                      <option value="sequential">Sequential</option>
                      <option value="concurrent">Concurrent</option>
                    </select>
                    <small id="simulation-mode-help" className="simulation-field-help">
                      {settings.mode === "concurrent"
                        ? "Active requests share each server’s CPU."
                        : "One request runs at a time per server; original validator compatible."}
                    </small>
                  </label>
                  <label>
                    Scheduling strategy
                    <select
                      aria-label="Scheduling strategy"
                      aria-describedby="simulation-strategy-help"
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
                    <small id="simulation-strategy-help" className="simulation-field-help">
                      {
                        {
                          first_fit: "Uses the first eligible server by ID.",
                          round_robin: "Rotates through eligible servers.",
                          least_connections:
                            "Chooses the eligible server with the fewest active requests.",
                          most_available_memory:
                            "Chooses the eligible server with the most free memory.",
                        }[settings.strategy]
                      }
                    </small>
                  </label>
                </div>
                <details
                  className="simulation-advanced"
                  open={advancedOpen}
                  onToggle={(event) => setAdvancedOpen(event.currentTarget.open)}
                >
                  <summary>
                    <SlidersHorizontalIcon className="summary-icon" size={16} aria-hidden="true" />{" "}
                    Advanced settings
                  </summary>
                  <p className="muted simulation-advanced-status">
                    Queue capacity: {settings.max_queue_size ?? "no limit"} · timeout:{" "}
                    {settings.queue_timeout_ticks === null
                      ? "off"
                      : `${settings.queue_timeout_ticks} ticks`}{" "}
                    · autoscaling: {settings.autoscaling.enabled ? "on" : "off"}
                  </p>
                  <div className="form-grid">
                    <label>
                      Queue capacity (blank: disabled)
                      <input
                        type="number"
                        min={0}
                        max={10_000}
                        step={1}
                        value={
                          settings.max_queue_size !== null &&
                          Number.isFinite(settings.max_queue_size)
                            ? settings.max_queue_size
                            : ""
                        }
                        onChange={(event) =>
                          change({
                            ...settings,
                            max_queue_size:
                              event.target.value === "" ? null : event.target.valueAsNumber,
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
                          Number.isFinite(settings.max_simulation_ticks)
                            ? settings.max_simulation_ticks
                            : ""
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
                      At most 64 logical servers, 10,000 input requests, 10 MiB CSV. Limits can be
                      lowered; domain admission may reject a configuration that exceeds the
                      installation envelope.
                    </p>
                    <div className="form-grid">
                      {(["max_samples", "max_fraction_bits", "max_metric_series"] as const).map(
                        (key) => (
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
                                {
                                  max_samples: 1_000_000,
                                  max_fraction_bits: 4096,
                                  max_metric_series: 10_000,
                                }[key]
                              }
                              step={1}
                              value={Number.isFinite(settings[key]) ? settings[key] : ""}
                              onChange={(event) =>
                                change({ ...settings, [key]: event.target.valueAsNumber })
                              }
                            />
                          </label>
                        ),
                      )}
                    </div>
                  </details>
                </details>
                <p className="muted simulation-effective">
                  Queue:{" "}
                  {settings.max_queue_size === null
                    ? "no capacity limit"
                    : `${settings.max_queue_size} requests`}{" "}
                  · timeout:{" "}
                  {settings.queue_timeout_ticks === null
                    ? "off"
                    : `${settings.queue_timeout_ticks} ticks`}{" "}
                  · autoscaling: {settings.autoscaling.enabled ? "on" : "off"}
                </p>
              </fieldset>
              <Feedback error={servers.error} />
              <Feedback error={launch.error} />
              {localError && <p role="alert">{localError}</p>}
              {launch.isError && (
                <p className="notice">
                  A retry with unchanged settings reuses the same idempotency key. Check history if
                  the previous response was interrupted.
                </p>
              )}
              {reviewed && (
                <div className="notice" role="status">
                  <h3 className="simulation-section-title">Review snapshot inputs</h3>
                  <p>
                    {dataset?.source_filename} · {dataset?.request_count.toLocaleString()} requests
                    · {settings.strategy.replaceAll("_", " ")} · {settings.mode}.
                  </p>
                  <p>
                    Queue capacity: {settings.max_queue_size ?? "no limit"} · timeout:{" "}
                    {settings.queue_timeout_ticks ?? "off"}
                    {settings.queue_timeout_ticks !== null ? " ticks" : ""} · autoscaling:{" "}
                    {settings.autoscaling.enabled ? "on" : "off"}.
                  </p>
                  <details className="simulation-review-details">
                    <summary>Server capacities and versions</summary>
                    <ul>
                      {servers.data?.items.map((server) => (
                        <li key={server.id}>
                          {server.id} · CPU {server.cpu_units_per_tick} · memory {server.mem_mb} MB
                          · starts {server.rate_limit_per_sec}/tick · version {server.version}
                        </li>
                      ))}
                    </ul>
                  </details>
                  <details className="simulation-review-details">
                    <summary>Configuration to submit</summary>
                    <pre>{JSON.stringify(settings, null, 2)}</pre>
                  </details>
                </div>
              )}
              <button
                type="submit"
                className="primary"
                disabled={!dataset || !servers.data?.items.length || registering}
              >
                {registering ? (
                  <CircleNotchIcon className="icon-spin" size={18} aria-hidden="true" />
                ) : (
                  <FlaskIcon size={18} aria-hidden="true" />
                )}
                {registering
                  ? "Registering run…"
                  : reviewed
                    ? "Confirm and queue run"
                    : "Review snapshot"}
                <ArrowRightIcon size={18} aria-hidden="true" />
              </button>
              {reviewed && !registering && (
                <button type="button" className="text-button" onClick={() => setReviewKey(null)}>
                  Back to configuration
                </button>
              )}
            </form>
            <aside className="simulation-cluster" aria-label="Current cluster">
              <div className="simulation-cluster-heading">
                <h3 className="simulation-section-title">
                  <HardDrivesIcon size={18} aria-hidden="true" /> Current cluster
                </h3>
                <Help text="Each run saves its inputs. Server edits affect future runs." />
              </div>
              {servers.isPending && <p role="status">Loading servers…</p>}
              {servers.data && (
                <>
                  <p>
                    {servers.data.items.length} servers ·{" "}
                    {servers.data.items
                      .reduce((sum, server) => sum + server.cpu_units_per_tick, 0)
                      .toLocaleString()}{" "}
                    CPU units/tick ·{" "}
                    {servers.data.items
                      .reduce((sum, server) => sum + server.mem_mb, 0)
                      .toLocaleString()}{" "}
                    MB
                  </p>
                  <ul className="cluster-preview">
                    {servers.data.items.map((server) => (
                      <li key={server.id}>
                        <span className="mono">{server.id}</span>
                        <span>
                          {server.cpu_units_per_tick} CPU · {server.mem_mb.toLocaleString()} MB ·{" "}
                          {server.rate_limit_per_sec} starts/tick
                        </span>
                      </li>
                    ))}
                  </ul>
                </>
              )}
              {servers.data?.items.length === 0 && (
                <p className="notice">Add a server in Servers before starting a simulation.</p>
              )}
              <p className="muted simulation-cluster-note">
                Logical time in ticks. No real HTTP traffic.
              </p>
              <p className="muted simulation-cluster-note">
                The run saves the latest server capacities when queued.
              </p>
            </aside>
          </div>
        </>
      )}
      {view === "history" && (
        <div>
          <h2 id="runs-heading">Run history</h2>
          <p className="muted">Inspect a saved experiment or copy its settings.</p>
          <Feedback error={history.error} />
          {history.isPending && <p role="status">Loading run history…</p>}
          {history.data?.items.length === 0 && <p>No simulations yet.</p>}
          <ul className="history compact-history">
            {history.data?.items
              .slice()
              .reverse()
              .map((run) => (
                <li key={run.id}>
                  <button
                    type="button"
                    className="compact-history-button"
                    aria-pressed={run.id === selected}
                    onClick={() => {
                      setSelected(run.id);
                      setAutoPlaybackRun(null);
                      setView("results");
                      cancel.reset();
                    }}
                  >
                    <strong>{run.configuration.strategy.replaceAll("_", " ")}</strong>
                    <span className="icon-label">
                      <StatusIcon status={run.status} />
                      {run.status.replaceAll("_", " ")}
                    </span>
                    <small title={run.id}>{run.id.slice(0, 8)}</small>
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
      )}
      {view === "results" && (
        <div className="simulation-results" aria-live="polite">
          <h2 id="runs-heading">Simulation results</h2>
          <Feedback error={detail.error} />
          {!selected && <p>Select a run to inspect its persisted status.</p>}
          {detail.isFetching && !detail.data && selected && <p>Loading run details…</p>}
          {detail.data && (
            <div className="panel result-summary">
              <p className="mono">{detail.data.id}</p>
              <p className="status icon-label">
                <StatusIcon status={detail.data.status} />
                {detail.data.status.replaceAll("_", " ")}
              </p>
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
              <details>
                <summary>Validation and saved configuration</summary>
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
                <p className="mono">Snapshot: {detail.data.snapshot_digest}</p>
                <p className="mono">Dataset: {detail.data.dataset_digest}</p>
                <p className="mono">Server snapshot: {detail.data.server_snapshot_digest}</p>
                <pre>{JSON.stringify(detail.data.configuration, null, 2)}</pre>
                <button
                  type="button"
                  onClick={() => {
                    change(structuredClone(detail.data.configuration));
                    setView("configure");
                  }}
                >
                  <CopyIcon size={16} aria-hidden="true" /> Copy settings for a new run
                </button>
              </details>
            </div>
          )}
          {detail.data && !terminal(detail.data.status) && (
            <div className="simulation-progress" role="status">
              <CircleNotchIcon className="icon-spin" size={20} aria-hidden="true" />
              <span>
                {detail.data.status === "queued"
                  ? "Waiting for a worker…"
                  : "Calculating simulation…"}{" "}
                Animated playback starts when saved results are ready.
              </span>
            </div>
          )}
        </div>
      )}
      {view === "results" && detail.data?.validation && (
        <Suspense fallback={<p role="status">Loading result views…</p>}>
          <ResultsExplorer
            key={detail.data.id}
            api={api}
            run={detail.data}
            terminalTick={detail.data.validation.terminal_tick}
            autoPlay={autoPlaybackRun === detail.data.id}
            onAutoPlayStarted={() => setAutoPlaybackRun(null)}
          />
        </Suspense>
      )}
      {view === "results" && detail.data && (
        <details onToggle={(event) => setShowEvents(event.currentTarget.open)}>
          <summary>Events and recovery details</summary>
          {showEvents && <LiveRun key={detail.data.id} api={api} run={detail.data} />}
        </details>
      )}
    </section>
  );
}

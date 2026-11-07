import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { lazy, Suspense, useState } from "react";
import { useDraftState, useDraftStorage } from "../../app/workspaceDrafts.js";
import { type Api, idempotencyKey } from "../../shared/api/api.js";
import {
  type Dataset,
  datasetSchema,
  type Settings,
  settingsSchema,
  terminal,
} from "../../shared/api/contracts.js";
import { ACTIVE_RUN_POLL_INTERVAL_MS } from "../../shared/api/polling.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import {
  CircleNotchIcon,
  ClockCounterClockwiseIcon,
  EyeIcon,
  FlaskIcon,
} from "../../shared/ui/Icons.js";
import { RunHistory } from "../history/RunHistory.js";
import { ClusterPreview } from "./ClusterPreview.js";
import { LiveRun } from "./LiveRun.js";
import { RunForm } from "./RunForm.js";
import { RunSummary } from "./RunSummary.js";
import { scalingPolicyError } from "./scalingPolicy.js";

const ResultsExplorer = lazy(() =>
  import("../results/ResultsExplorer.js").then((module) => ({ default: module.ResultsExplorer })),
);

/** Coordinate configuration, idempotent submission, history and saved result views. */
export function RunWorkspace({
  api,
  dataset,
  onChooseDataset,
  onSelectDataset,
}: {
  api: Api;
  dataset: Dataset | null;
  onChooseDataset?: () => void;
  onSelectDataset?: (dataset: Dataset) => void;
}) {
  const client = useQueryClient();
  const storage = useDraftStorage();
  const [, setCursor] = useDraftState({ storage, name: "runCursor" });
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
  const detail = useQuery({
    queryKey: ["run", selected],
    queryFn: ({ signal }) => api.run(selected ?? "", signal),
    enabled: selected !== null && view === "results",
    refetchInterval: (query) =>
      query.state.data && terminal(query.state.data.status) ? false : ACTIVE_RUN_POLL_INTERVAL_MS,
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
    <section aria-label="Simulations" className="simulation-workspace">
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
            <RunForm
              dataset={dataset}
              settings={settings}
              servers={servers}
              submission={{
                registering,
                reviewed,
                localError,
                advancedOpen,
                error: launch.error,
                isError: launch.isError,
              }}
              onChange={change}
              onAdvancedOpenChange={setAdvancedOpen}
              onReviewReset={() => setReviewKey(null)}
              onChooseDataset={onChooseDataset}
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
            />
            <ClusterPreview servers={servers.data} pending={servers.isPending} />
          </div>
        </>
      )}
      {view === "history" && (
        <RunHistory
          api={api}
          kind="logical"
          onOpen={({ id, playback }) => {
            setSelected(id);
            setAutoPlaybackRun(playback ? id : null);
            setView("results");
            cancel.reset();
          }}
          onRepeat={async (entry) => {
            if (registering)
              throw new Error("Wait for the current registration before preparing another run.");
            const [saved, input] = await Promise.all([
              api.run(entry.id, new AbortController().signal),
              api.request(`datasets/${encodeURIComponent(entry.dataset_id)}`, datasetSchema),
            ]);
            onSelectDataset?.(input);
            change(structuredClone(saved.configuration));
            setAutoPlaybackRun(null);
            setView("configure");
            setReviewKey(null);
          }}
        />
      )}
      {view === "results" && (
        <div className="simulation-results" aria-live="polite">
          <h2 id="runs-heading">Simulation results</h2>
          <Feedback error={detail.error} />
          {!selected && <p>Select a run to inspect its persisted status.</p>}
          {detail.isFetching && !detail.data && selected && <p>Loading run details…</p>}
          {detail.data && (
            <RunSummary
              run={detail.data}
              cancelling={cancel.isPending}
              cancelError={cancel.error}
              onCancel={() => {
                if (detail.data) cancel.mutate(detail.data.id);
              }}
              onCopy={(configuration) => {
                change(configuration);
                setView("configure");
              }}
            />
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

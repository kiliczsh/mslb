import { useIsMutating, useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useDraftState, useDraftStorage } from "../../app/workspaceDrafts.js";
import { type Api, ApiError, idempotencyKey } from "../../shared/api/api.js";
import { type Dataset, datasetSchema } from "../../shared/api/contracts.js";
import { dockerCreateSchema } from "../../shared/api/dockerContracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import {
  CircleNotchIcon,
  ClockCounterClockwiseIcon,
  EyeIcon,
  SlidersHorizontalIcon,
} from "../../shared/ui/Icons.js";
import { RunHistory } from "../history/RunHistory.js";
import { DockerControls } from "./DockerControls.js";
import { DockerDetail } from "./DockerDetail.js";

export function DockerWorkspace({
  api,
  dataset,
  onChooseDataset,
  onSelectDataset,
}: {
  api: Api;
  dataset: Dataset | null;
  onChooseDataset: () => void;
  onSelectDataset?: (dataset: Dataset) => void;
}) {
  const storage = useDraftStorage(),
    client = useQueryClient();
  const [draft, setDraft] = useDraftState({ storage, name: "docker" });
  const [view, setView] = useState<"configure" | "history" | "results">(
    draft.selected ? "results" : "configure",
  );
  const [error, setError] = useState("");
  const [review, setReview] = useState<string | null>(null);
  const busy = useIsMutating({ mutationKey: ["docker-launch"] }) > 0;
  const launch = useMutation({
    mutationKey: ["docker-launch"],
    mutationFn: async (ticket: NonNullable<typeof draft.retry>) => {
      const body = dockerCreateSchema.parse(JSON.parse(ticket.body));
      const receipt = await api.dockerSubmit({ body, key: ticket.key });
      return { receipt, ticket };
    },
    onError: (failure, ticket) => {
      if (
        failure instanceof ApiError &&
        ["VALIDATION_ERROR", "RESOURCE_LIMIT_EXCEEDED"].includes(failure.code)
      ) {
        setDraft((current) =>
          current.retry?.key === ticket.key ? { ...current, retry: null } : current,
        );
        setReview(null);
      }
    },
    onSuccess: async ({ receipt, ticket }) => {
      setDraft((current) =>
        current.retry?.key !== ticket.key
          ? current
          : {
              ...current,
              retry: null,
              cursor: null,
              selected:
                current.generation === ticket.generation ? receipt.run_id : current.selected,
            },
      );
      setReview(null);
      setView("results");
      await client.invalidateQueries({ queryKey: ["docker-runs"] });
    },
  });
  const bodyKey = JSON.stringify({ dataset_id: dataset?.id, ...draft.settings });
  const reviewed = review === bodyKey;
  function start() {
    if (busy) return;
    if (draft.retry) {
      launch.mutate(draft.retry);
      return;
    }
    if (!dataset) return;
    const parsed = dockerCreateSchema.safeParse({ dataset_id: dataset.id, ...draft.settings });
    if (!parsed.success) {
      setError(
        parsed.error.issues
          .map((i) => i.message)
          .slice(0, 4)
          .join("; "),
      );
      return;
    }
    if (dataset.request_count > 10_000 || dataset.latest_arrival >= parsed.data.duration_seconds) {
      setError(
        "Docker traffic supports at most 10,000 requests and arrivals before the run duration limit.",
      );
      return;
    }
    if (!reviewed) {
      setReview(bodyKey);
      setError("");
      return;
    }
    const ticket = {
      body: JSON.stringify(parsed.data),
      key: idempotencyKey(),
      generation: draft.generation,
    };
    setDraft((current) => ({ ...current, retry: ticket }));
    launch.mutate(ticket);
  }
  const datasetIssue = !dataset
    ? "Choose a CSV to configure real HTTP traffic."
    : dataset.request_count > 10_000
      ? "Docker traffic supports at most 10,000 requests."
      : dataset.latest_arrival >= 120
        ? `This CSV has arrivals through ${dataset.latest_arrival}s. Choose arrivals below 120s for Docker traffic.`
        : dataset.latest_arrival >= draft.settings.duration_seconds
          ? `Increase the duration above ${dataset.latest_arrival}s to include every arrival.`
          : null;
  return (
    <section className="docker-workspace docker-compact" aria-label="Docker traffic">
      <nav className="view-tabs" aria-label="Docker views">
        <button
          type="button"
          aria-pressed={view === "configure"}
          onClick={() => setView("configure")}
        >
          <SlidersHorizontalIcon size={16} aria-hidden="true" /> New traffic run
        </button>
        <button type="button" aria-pressed={view === "history"} onClick={() => setView("history")}>
          <ClockCounterClockwiseIcon size={16} aria-hidden="true" /> Run history
        </button>
        <button
          type="button"
          disabled={!draft.selected}
          aria-pressed={view === "results"}
          onClick={() => setView("results")}
        >
          <EyeIcon size={16} aria-hidden="true" /> Live / Results
        </button>
      </nav>
      {view === "configure" && (
        <>
          <div className="simulation-intro">
            <h2>New traffic run</h2>
            <p className="muted">
              Send real HTTP requests to private Docker replicas. CSV arrival times are seconds.
            </p>
          </div>
          <div className="docker-setup">
            <form
              className="run-form"
              noValidate
              onSubmit={(event) => {
                event.preventDefault();
                start();
              }}
            >
              <div className="simulation-dataset">
                <p>
                  {dataset ? (
                    <>
                      <strong>{dataset.source_filename}</strong> ·{" "}
                      {dataset.request_count.toLocaleString()} requests · last arrival{" "}
                      {dataset.latest_arrival}s
                    </>
                  ) : (
                    "Select an input dataset."
                  )}
                </p>
                <button
                  type="button"
                  className="text-button"
                  disabled={busy || draft.retry !== null}
                  onClick={onChooseDataset}
                >
                  {dataset ? "Change dataset" : "Choose dataset"}
                </button>
              </div>
              {datasetIssue && (
                <p className="notice" role="status">
                  {datasetIssue}
                </p>
              )}
              {!datasetIssue && (
                <p className="docker-compatible">
                  CSV fits the current duration and request limits.
                </p>
              )}
              <DockerControls
                settings={draft.settings}
                disabled={busy || draft.retry !== null}
                change={(settings) => {
                  setDraft((current) => ({ ...current, settings }));
                  setReview(null);
                  setError("");
                  launch.reset();
                }}
              />
              {reviewed && !draft.retry && (
                <section className="notice" aria-label="Reviewed Docker snapshot">
                  <h3>Review traffic run</h3>
                  <p>
                    {dataset?.source_filename} · {draft.settings.strategy.replaceAll("_", " ")} ·{" "}
                    {draft.settings.initial_replicas} starting replicas · up to{" "}
                    {draft.settings.global_inflight} simultaneous requests · maximum{" "}
                    {draft.settings.duration_seconds}s.
                  </p>
                  <p>
                    Autoscaling:{" "}
                    {draft.settings.physical_scaling.enabled
                      ? `on (${draft.settings.min_replicas}–${draft.settings.max_replicas} replicas)`
                      : "off"}
                    .
                  </p>
                  <details>
                    <summary>Exact settings</summary>
                    <pre>{JSON.stringify(draft.settings, null, 2)}</pre>
                  </details>
                </section>
              )}
              {draft.retry && (
                <p role="status">
                  Recover the previous registration before changing settings. Retry uses the same
                  saved request.
                </p>
              )}
              {error && <p role="alert">{error}</p>}
              <Feedback error={launch.error} />
              <div className="actions">
                <button
                  type="submit"
                  className="primary"
                  disabled={busy || (!draft.retry && datasetIssue !== null)}
                >
                  {busy ? (
                    <>
                      <CircleNotchIcon className="icon-spin" size={16} aria-hidden="true" />{" "}
                      Registering…
                    </>
                  ) : draft.retry ? (
                    "Recover registration"
                  ) : reviewed ? (
                    "Start traffic run"
                  ) : (
                    "Review traffic run"
                  )}
                </button>
                {reviewed && !draft.retry && (
                  <button type="button" disabled={busy} onClick={() => setReview(null)}>
                    Back to settings
                  </button>
                )}
              </div>
            </form>
            <aside className="simulation-cluster" aria-label="Traffic summary">
              <h3 className="simulation-section-title">Traffic summary</h3>
              <dl className="docker-summary-values">
                <div>
                  <dt>Starting replicas</dt>
                  <dd>{draft.settings.initial_replicas}</dd>
                </div>
                <div>
                  <dt>Simultaneous requests</dt>
                  <dd>{draft.settings.global_inflight}</dd>
                </div>
                <div>
                  <dt>Duration limit</dt>
                  <dd>{draft.settings.duration_seconds}s</dd>
                </div>
                <div>
                  <dt>Autoscaling</dt>
                  <dd>
                    {draft.settings.physical_scaling.enabled
                      ? `${draft.settings.min_replicas}–${draft.settings.max_replicas} replicas`
                      : "Off"}
                  </dd>
                </div>
              </dl>
              <p className="muted">
                The Servers workspace configures logical simulations. This run uses its own Docker
                replica settings.
              </p>
            </aside>
          </div>
        </>
      )}
      {view === "history" && (
        <RunHistory
          api={api}
          kind="docker"
          onOpen={({ id }) => {
            setDraft((current) => ({
              ...current,
              selected: id,
              generation: current.generation + 1,
            }));
            setView("results");
          }}
          onRepeat={async (entry) => {
            if (draft.retry || busy)
              throw new ApiError("Recover the pending registration before preparing another run.");
            const [saved, input] = await Promise.all([
              api.dockerRun({ id: entry.id, signal: new AbortController().signal }),
              api.request(`datasets/${encodeURIComponent(entry.dataset_id)}`, datasetSchema),
            ]);
            onSelectDataset?.(input);
            setDraft((current) => ({
              ...current,
              settings: structuredClone(saved.run.configuration),
              generation: current.generation + 1,
            }));
            setReview(null);
            setError("");
            setView("configure");
          }}
        />
      )}
      {view === "results" && draft.selected && (
        <DockerDetail key={draft.selected} api={api} id={draft.selected} />
      )}
    </section>
  );
}

import { useIsMutating, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useDraftState, useDraftStorage } from "../../app/workspaceDrafts.js";
import { type Api, ApiError, idempotencyKey } from "../../shared/api/api.js";
import type { Dataset } from "../../shared/api/contracts.js";
import { dockerCreateSchema } from "../../shared/api/dockerContracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import { DockerControls } from "./DockerControls.js";
import { DockerDetail } from "./DockerDetail.js";

export function DockerWorkspace({
  api,
  dataset,
  onChooseDataset,
}: {
  api: Api;
  dataset: Dataset | null;
  onChooseDataset: () => void;
}) {
  const storage = useDraftStorage(),
    client = useQueryClient();
  const [draft, setDraft] = useDraftState({ storage, name: "docker" });
  const [error, setError] = useState("");
  const [review, setReview] = useState<string | null>(null);
  const busy = useIsMutating({ mutationKey: ["docker-launch"] }) > 0;
  const history = useQuery({
    queryKey: ["docker-runs", draft.cursor],
    queryFn: ({ signal }) => api.dockerRuns({ cursor: draft.cursor, signal }),
    refetchInterval: 3000,
  });
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
  return (
    <div className="docker-workspace">
      <header className="section-heading">
        <div>
          <p className="eyebrow">Operational mode</p>
          <h2>Docker traffic</h2>
          <p>
            Run real HTTP work against bounded private backends. Arrivals use elapsed seconds; CPU
            work and container measurements have their own units.
          </p>
        </div>
      </header>
      <div className="setup-layout">
        <form
          className="run-form"
          onSubmit={(event) => {
            event.preventDefault();
            start();
          }}
        >
          <h3>Input dataset</h3>
          <p>
            {dataset
              ? `${dataset.source_filename} · ${dataset.request_count} requests`
              : "Select a dataset before launching Docker traffic."}
          </p>
          <button type="button" onClick={onChooseDataset}>
            Choose Docker dataset
          </button>
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
            <section aria-label="Reviewed Docker snapshot">
              <h3>Review Docker snapshot</h3>
              <p>
                Dataset: <span className="digest">{dataset?.digest}</span>
              </p>
              <p>
                {draft.settings.strategy.replaceAll("_", " ")} · {draft.settings.initial_replicas}{" "}
                initial backends ·{draft.settings.global_inflight} maximum HTTP reservations ·{" "}
                {draft.settings.duration_seconds} seconds maximum.
              </p>
            </section>
          )}
          {draft.retry && (
            <p role="status">
              A frozen registration ticket is retained. Retry safely to recover its receipt;
              settings stay frozen until the response is known.
            </p>
          )}
          {error && <p role="alert">{error}</p>}
          <Feedback error={launch.error} />
          <div className="actions">
            <button type="submit" className="primary" disabled={busy || (!dataset && !draft.retry)}>
              {busy
                ? "Registering Docker run…"
                : draft.retry
                  ? "Retry Docker registration"
                  : reviewed
                    ? "Queue Docker run"
                    : "Review Docker snapshot"}
            </button>
            {reviewed && !draft.retry && (
              <button type="button" onClick={() => setReview(null)}>
                Back to Docker settings
              </button>
            )}
          </div>
        </form>
        <section className="run-preview" aria-label="Docker run history">
          <h3>Docker history</h3>
          <Feedback error={history.error} />
          {history.data?.items.length === 0 && <p>No Docker runs on this page.</p>}
          <ul className="docker-history">
            {history.data?.items.map((run) => (
              <li key={run.id}>
                <button
                  type="button"
                  className="docker-history-button"
                  aria-pressed={draft.selected === run.id}
                  onClick={() =>
                    setDraft((current) => ({
                      ...current,
                      selected: run.id,
                      generation: current.generation + 1,
                    }))
                  }
                >
                  <span className="digest">{run.id}</span>
                  <span>
                    {run.status} · {run.configuration.strategy.replaceAll("_", " ")}
                  </span>
                </button>
              </li>
            ))}
          </ul>
          <div className="actions">
            <button
              type="button"
              disabled={!draft.cursor}
              onClick={() => setDraft((current) => ({ ...current, cursor: null }))}
            >
              First Docker page
            </button>
            <button
              type="button"
              disabled={!history.data?.next_cursor}
              onClick={() =>
                setDraft((current) => ({ ...current, cursor: history.data?.next_cursor ?? null }))
              }
            >
              Next Docker page
            </button>
          </div>
        </section>
      </div>
      {draft.selected && <DockerDetail key={draft.selected} api={api} id={draft.selected} />}
    </div>
  );
}

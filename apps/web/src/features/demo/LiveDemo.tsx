import {
  useIsMutating,
  useMutation,
  useQueries,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import { useState } from "react";
import { useDraftState, useDraftStorage } from "../../app/workspaceDrafts.js";
import { type Api, idempotencyKey } from "../../shared/api/api.js";
import { type Settings, terminal } from "../../shared/api/contracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import { DemoComparison } from "./DemoComparison.js";
import { DemoLoading } from "./DemoLoading.js";
import { DemoResults } from "./DemoResults.js";
import {
  type DemoSession,
  demoInput,
  demoRecipes,
  demoStrategies,
  strategyDescriptions,
  strategyLabels,
} from "./demoState.js";

export function LiveDemo({
  api,
  onServers,
  onOpenRun,
}: {
  api: Api;
  onServers: () => void;
  onOpenRun: (id: string) => void;
}) {
  const storage = useDraftStorage();
  const [draft, setDraft] = useDraftState({ storage, name: "demo" });
  const client = useQueryClient();
  const [view, setView] = useState<"playback" | "comparison">("playback");
  const [selectedStrategy, setSelectedStrategy] = useState<Settings["strategy"]>("first_fit");
  const registering = useIsMutating({ mutationKey: ["demo-launch"] }) > 0;
  const servers = useQuery({ queryKey: ["servers"], queryFn: ({ signal }) => api.servers(signal) });
  const plan = servers.data?.items.length
    ? demoInput({ servers: servers.data.items, kind: draft.recipe })
    : null;
  const recipe = demoRecipes.find((value) => value.id === draft.recipe) ?? demoRecipes[0];
  const previewSettings = draft.session?.settings ?? plan?.settings;
  const attempts = draft.session?.attempts ?? [];
  const ids = attempts.flatMap((attempt) => (attempt.runId ? [attempt.runId] : []));
  const runs = useQueries({
    queries: ids.map((id) => ({
      queryKey: ["run", id],
      queryFn: ({ signal }: { signal: AbortSignal }) => api.run(id, signal),
      refetchInterval: (query: { state: { data: Awaited<ReturnType<Api["run"]>> | undefined } }) =>
        query.state.data && terminal(query.state.data.status) ? (false as const) : 1500,
    })),
  });
  const first = runs[0]?.data;
  const selectedAttempt = attempts.find((attempt) => attempt.strategy === selectedStrategy);
  const selectedQuery = selectedAttempt?.runId
    ? runs[ids.indexOf(selectedAttempt.runId)]
    : undefined;
  const selectedRun = selectedQuery?.data;
  const allTerminal = runs.length > 0 && runs.every((run) => run.data && terminal(run.data.status));
  const unresolved = attempts.some((attempt) => !attempt.runId);
  const locked = registering || unresolved || (ids.length > 0 && !allTerminal);
  const allCompleted = runs.length === 4 && runs.every((run) => run.data?.status === "completed");
  const waitingForRun =
    ids.length > 0 && !first?.error_code && !runs[0]?.error && (!first || !terminal(first.status));
  const demoBusy = registering || waitingForRun;
  const comparison = useQuery({
    queryKey: ["comparison", ids],
    queryFn: ({ signal }) => api.compare(ids, signal),
    enabled: allCompleted && view === "comparison",
  });
  const launch = useMutation({
    mutationKey: ["demo-launch"],
    mutationFn: async ({
      compare,
      strategy,
    }: {
      compare: boolean;
      strategy?: Settings["strategy"];
    }) => {
      let session = storage.current.demo.session;
      if (!session) {
        if (!plan) throw new Error("Configure at least one server first.");
        session = {
          generation: idempotencyKey(),
          csv: plan.csv,
          settings: plan.settings,
          datasetId: null,
          attempts: [{ strategy: "first_fit", key: idempotencyKey(), runId: null }],
        };
      }
      if (compare) {
        session = {
          ...session,
          attempts: demoStrategies.map(
            (strategy) =>
              session?.attempts.find((attempt) => attempt.strategy === strategy) ?? {
                strategy,
                key: idempotencyKey(),
                runId: null,
              },
          ),
        };
      } else if (strategy && !session.attempts.some((attempt) => attempt.strategy === strategy)) {
        session = {
          ...session,
          attempts: [...session.attempts, { strategy, key: idempotencyKey(), runId: null }],
        };
      }
      const generation = session.generation;
      function persist(next: DemoSession) {
        const current = storage.current.demo.session;
        if (current && current.generation !== generation) return;
        setDraft((value) => ({ ...value, session: next }));
      }
      persist(session);
      if (!session.datasetId) {
        const dataset = await api.upload(
          new File([session.csv], `demo-${draft.recipe}.csv`, { type: "text/csv" }),
        );
        session = { ...session, datasetId: dataset.id };
        persist(session);
      }
      const datasetId = session.datasetId;
      if (!datasetId) throw new Error("The demo dataset was not registered.");
      for (const attempt of session.attempts) {
        if (attempt.runId) continue;
        const accepted = await api.submit(
          datasetId,
          { ...session.settings, strategy: attempt.strategy },
          attempt.key,
        );
        session = {
          ...session,
          attempts: session.attempts.map((value) =>
            value.key === attempt.key ? { ...value, runId: accepted.run_id } : value,
          ),
        };
        persist(session);
      }
      await client.invalidateQueries({ queryKey: ["runs"] });
    },
  });
  return (
    <section className="guided-demo" aria-label="Guided live demo">
      <div className="section-heading">
        <div>
          <p className="eyebrow">Choose → run → inspect → compare</p>
          <h2>A clearer view of every request.</h2>
          <p>A small real run on your current cluster. Saved results, ready to inspect.</p>
        </div>
      </div>
      <div className="demo-layout">
        <aside className="demo-setup" aria-label="Demo setup">
          <fieldset disabled={locked}>
            <legend>1 · Choose a workload</legend>
            {demoRecipes.map((recipe) => (
              <label className="demo-recipe" key={recipe.id}>
                <input
                  type="radio"
                  name="demo-recipe"
                  value={recipe.id}
                  checked={draft.recipe === recipe.id}
                  onChange={() => {
                    setDraft({ recipe: recipe.id, session: null });
                    setSelectedStrategy("first_fit");
                    launch.reset();
                  }}
                />
                <span className="demo-recipe-copy">
                  <strong>{recipe.title}</strong>
                  <small>{recipe.description}</small>
                </span>
              </label>
            ))}
          </fieldset>
          <Feedback error={servers.error ?? launch.error} />
          {plan ? (
            <p className="muted">
              {plan.count} requests · {servers.data?.items.length} current servers · concurrent CPU
              sharing. Capacities determine the outcome.
            </p>
          ) : (
            <p>Configure at least one server to begin.</p>
          )}
          <button
            type="button"
            className="primary demo-action"
            disabled={
              ids.length > 0 && !unresolved ? locked : (!plan && !draft.session) || registering
            }
            onClick={() => {
              if (ids.length > 0 && !unresolved) {
                setDraft((value) => ({ ...value, session: null }));
                setSelectedStrategy("first_fit");
                launch.reset();
              } else {
                setView(attempts.length === 4 ? "comparison" : "playback");
                launch.mutate({ compare: attempts.length === 4 });
              }
            }}
          >
            {registering
              ? "Registering demo…"
              : unresolved
                ? "Retry same submission"
                : ids.length > 0
                  ? "New demo"
                  : "2 · Run demo"}
          </button>
          {unresolved && !registering && (
            <p className="notice">
              The submission may have reached the server. Retry keeps its original workload and
              keys, even after navigation.
            </p>
          )}
          <button className="demo-action" type="button" onClick={onServers}>
            Manage current servers
          </button>
        </aside>
        <div className="demo-output">
          {demoBusy && first?.status !== "completed" && (
            <DemoLoading
              title={registering ? "Preparing your demo…" : "Running your workload…"}
              detail={
                registering
                  ? "Saving the workload and registering the run."
                  : first?.status === "queued"
                    ? "Your run is queued. Results will appear here when it completes."
                    : "Processing requests and saving results. This view updates automatically."
              }
            />
          )}
          {!demoBusy && first?.status !== "completed" && (
            <section className="demo-empty" aria-label="Workload preview">
              <p className="eyebrow">What will run?</p>
              <h3>{recipe.title}</h3>
              <p>{recipe.preview}</p>
              {previewSettings && (
                <p className="muted">
                  {strategyLabels[previewSettings.strategy]} · {previewSettings.mode} mode ·
                  simulated autoscaling {previewSettings.autoscaling.enabled ? "on" : "off"}. Server
                  configuration is unchanged.
                </p>
              )}
              <pre className="demo-csv">
                {draft.session?.csv ?? plan?.csv ?? "No workload yet."}
              </pre>
              <p className="muted">Your saved results appear here when the run completes.</p>
            </section>
          )}
          {runs.length > 0 && (
            <p role="status">
              Saved runs: {runs.filter((run) => run.data?.status === "completed").length} /{" "}
              {runs.length} completed.
            </p>
          )}
          {runs.map((run, index) => (
            <div key={ids[index]}>
              <Feedback error={run.error} />
              {run.data?.error_code && (
                <p role="alert">
                  {strategyLabels[run.data.configuration.strategy]} · {run.data.status} ·{" "}
                  {run.data.error_code}
                </p>
              )}
            </div>
          ))}
          {first?.status === "completed" && first.validation && (
            <>
              {attempts.length === 4 && (
                <nav aria-label="Guided result views">
                  <button
                    type="button"
                    aria-pressed={view === "playback"}
                    onClick={() => setView("playback")}
                  >
                    Saved playback
                  </button>
                  <button
                    type="button"
                    aria-pressed={view === "comparison"}
                    onClick={() => setView("comparison")}
                  >
                    Strategy comparison
                  </button>
                </nav>
              )}
              {view === "playback" && (
                <>
                  <div className="actions">
                    <label>
                      Playback strategy
                      <select
                        value={selectedStrategy}
                        disabled={locked}
                        onChange={(event) => {
                          const strategy = demoStrategies.find(
                            (value) => value === event.target.value,
                          );
                          if (strategy) setSelectedStrategy(strategy);
                        }}
                      >
                        {demoStrategies.map((strategy) => (
                          <option key={strategy} value={strategy}>
                            {strategyLabels[strategy]}
                          </option>
                        ))}
                      </select>
                    </label>
                    {!selectedAttempt && (
                      <button
                        type="button"
                        className="primary"
                        disabled={locked}
                        onClick={() =>
                          launch.mutate({ compare: false, strategy: selectedStrategy })
                        }
                      >
                        Run {strategyLabels[selectedStrategy]}
                      </button>
                    )}
                  </div>
                  <p className="muted">{strategyDescriptions[selectedStrategy]}</p>
                  {!selectedAttempt && (
                    <p>Run this strategy on the same saved workload to see its request flow.</p>
                  )}
                  {(registering ||
                    (selectedAttempt?.runId &&
                      !selectedQuery?.error &&
                      (!selectedRun || !terminal(selectedRun.status)))) && (
                    <DemoLoading
                      title={`Running ${strategyLabels[selectedStrategy]}…`}
                      detail="Using the same saved workload and settings. Results appear here automatically."
                    />
                  )}
                  {selectedRun?.status === "completed" && selectedRun.validation && (
                    <DemoResults
                      key={selectedRun.id}
                      api={api}
                      runId={selectedRun.id}
                      terminalTick={selectedRun.validation.terminal_tick}
                    />
                  )}
                </>
              )}
              <div className="actions">
                <button
                  type="button"
                  className="primary"
                  disabled={locked || attempts.length === 4}
                  onClick={() => {
                    setView("comparison");
                    launch.mutate({ compare: true });
                  }}
                >
                  4 · Compare all four strategies
                </button>
                <button
                  type="button"
                  disabled={!selectedRun}
                  onClick={() => selectedRun && onOpenRun(selectedRun.id)}
                >
                  Open full results and downloads
                </button>
              </div>
            </>
          )}
          <Feedback error={comparison.error} />
          {view === "comparison" && !allCompleted && (
            <p role="status">The comparison appears after all four real runs complete.</p>
          )}
          {view === "comparison" &&
            !comparison.error &&
            !comparison.data &&
            (registering ||
              runs.some((run) => run.data && !terminal(run.data.status)) ||
              (allCompleted && comparison.isPending)) && (
              <DemoLoading
                title="Comparing strategies…"
                detail={`${runs.filter((run) => run.data?.status === "completed").length} of 4 runs completed. Preparing the saved comparison.`}
              />
            )}
          {view === "comparison" && comparison.data && (
            <DemoComparison comparison={comparison.data} onOpenRun={onOpenRun} />
          )}
        </div>
      </div>
    </section>
  );
}

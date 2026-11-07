import type { Api } from "../../shared/api/api.js";
import { terminal } from "../../shared/api/contracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import { EyeIcon, FileCsvIcon, InfoIcon, ScalesIcon } from "../../shared/ui/Icons.js";
import { DemoComparison } from "./DemoComparison.js";
import { DemoLoading } from "./DemoLoading.js";
import { DemoResults } from "./DemoResults.js";
import { DemoSetup } from "./DemoSetup.js";
import { demoStrategies, strategyDescriptions, strategyLabels } from "./demoState.js";

import { useLiveDemo } from "./useLiveDemo.js";

/** Present the guided workload, durable progress and saved strategy comparison. */
export function LiveDemo({
  api,
  onServers,
  onOpenRun,
}: {
  api: Api;
  onServers: () => void;
  onOpenRun: (id: string) => void;
}) {
  const demo = useLiveDemo({ api });
  const {
    draft,
    plan,
    recipe,
    previewSettings,
    attempts,
    ids,
    runs,
    first,
    selectedAttempt,
    selectedQuery,
    selectedRun,
    locked,
    allCompleted,
    readingRun,
    demoBusy,
    comparison,
    launch,
    view,
    setView,
    selectedStrategy,
    setSelectedStrategy,
    registering,
  } = demo;
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
        <DemoSetup demo={demo} onServers={onServers} />
        <div className="demo-output">
          {demoBusy && first?.status !== "completed" && (
            <DemoLoading
              title={
                registering
                  ? "Preparing your demo…"
                  : readingRun
                    ? "Loading saved demo…"
                    : "Running your workload…"
              }
              detail={
                registering
                  ? "Saving the workload and registering the run."
                  : readingRun
                    ? "Reading the status of your saved run."
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
              <details>
                <summary>
                  <FileCsvIcon className="summary-icon" size={16} aria-hidden="true" /> View
                  workload CSV
                </summary>
                <pre className="demo-csv">
                  {draft.session?.csv ?? plan?.csv ?? "No workload yet."}
                </pre>
              </details>
              <p className="muted">Your saved results appear here when the run completes.</p>
            </section>
          )}
          {runs.length > 0 && (
            <p role="status">
              Saved runs: {runs.filter((run) => run.data?.status === "completed").length} /{" "}
              {runs.length} completed.
            </p>
          )}
          {runs.some((run) => run.error) && (
            <div className="actions">
              <p role="status">Some results could not be loaded.</p>
              <button
                type="button"
                onClick={() => {
                  for (const run of runs) if (run.error) void run.refetch();
                }}
              >
                Retry loading results
              </button>
            </div>
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
              {attempts.length === demoStrategies.length && (
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
                  <div className="actions strategy-toolbar">
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
                  <details className="strategy-help">
                    <summary>
                      <InfoIcon className="summary-icon" size={16} aria-hidden="true" /> How{" "}
                      {strategyLabels[selectedStrategy]} works
                    </summary>
                    <p className="muted">{strategyDescriptions[selectedStrategy]}</p>
                  </details>
                  {!selectedAttempt && (
                    <p>Run this strategy on the same saved workload to see its request flow.</p>
                  )}
                  {(registering ||
                    (selectedAttempt?.runId &&
                      !selectedQuery?.error &&
                      (!selectedRun || !terminal(selectedRun.status)))) && (
                    <DemoLoading
                      title={
                        selectedAttempt?.runId && !selectedRun
                          ? "Loading saved run…"
                          : `Running ${strategyLabels[selectedStrategy]}…`
                      }
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
                  disabled={locked}
                  onClick={() => {
                    setView("comparison");
                    if (attempts.length < 4) launch.mutate({ compare: true });
                  }}
                >
                  <ScalesIcon size={16} aria-hidden="true" /> Compare all strategies
                </button>
                <button
                  type="button"
                  disabled={!selectedRun}
                  onClick={() => selectedRun && onOpenRun(selectedRun.id)}
                >
                  <EyeIcon size={16} aria-hidden="true" /> Full results
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

import { Feedback } from "../../shared/ui/Feedback.js";
import { HardDrivesIcon, PlayIcon } from "../../shared/ui/Icons.js";
import { demoRecipes, demoStrategies } from "./demoState.js";
import type { useLiveDemo } from "./useLiveDemo.js";

type DemoSetupState = Pick<
  ReturnType<typeof useLiveDemo>,
  | "draft"
  | "setDraft"
  | "servers"
  | "plan"
  | "attempts"
  | "ids"
  | "unresolved"
  | "locked"
  | "launch"
  | "setView"
  | "setSelectedStrategy"
  | "registering"
>;

/** Choose a workload or resume its original interrupted submission before launching another. */
export function DemoSetup({ demo, onServers }: { demo: DemoSetupState; onServers: () => void }) {
  const {
    draft,
    setDraft,
    servers,
    plan,
    attempts,
    ids,
    unresolved,
    locked,
    launch,
    setView,
    setSelectedStrategy,
    registering,
  } = demo;
  return (
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
        <p role="status">
          {servers.isPending ? "Loading servers…" : "Configure at least one server to begin."}
        </p>
      )}
      <button
        type="button"
        className="primary demo-action"
        disabled={ids.length > 0 && !unresolved ? locked : (!plan && !draft.session) || registering}
        onClick={() => {
          if (ids.length > 0 && !unresolved) {
            setDraft((value) => ({ ...value, session: null }));
            setSelectedStrategy("first_fit");
            launch.reset();
          } else {
            setView(attempts.length === demoStrategies.length ? "comparison" : "playback");
            launch.mutate({ compare: attempts.length === demoStrategies.length });
          }
        }}
      >
        <PlayIcon size={16} aria-hidden="true" />
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
          The submission may have reached the server. Retry keeps its original workload and keys,
          even after navigation.
        </p>
      )}
      <button className="demo-action" type="button" onClick={onServers}>
        <HardDrivesIcon size={16} aria-hidden="true" /> Manage servers
      </button>
    </aside>
  );
}

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
import { ACTIVE_RUN_POLL_INTERVAL_MS } from "../../shared/api/polling.js";
import { type DemoSession, demoInput, demoRecipes, demoStrategies } from "./demoState.js";

/** Resume a frozen demo session, preserving idempotency keys across interrupted submissions. */
export function useLiveDemo({ api }: { api: Api }) {
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
        query.state.data && terminal(query.state.data.status)
          ? (false as const)
          : ACTIVE_RUN_POLL_INTERVAL_MS,
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
  const allCompleted =
    runs.length === demoStrategies.length && runs.every((run) => run.data?.status === "completed");
  const waitingForRun =
    ids.length > 0 && !first?.error_code && !runs[0]?.error && (!first || !terminal(first.status));
  const readingRun = ids.length > 0 && !first;
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
      // Ignore late responses from a different saved demo generation.
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
  return {
    draft,
    setDraft,
    servers,
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
    unresolved,
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
  };
}

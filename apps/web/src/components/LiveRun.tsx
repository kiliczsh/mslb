import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import type { Api } from "../lib/api.js";
import type { Run } from "../lib/contracts.js";
import { type LiveState, watchRun } from "../lib/eventStream.js";
import { VirtualList } from "./VirtualList.js";

export function LiveRun({ api, run }: { api: Api; run: Run }) {
  const client = useQueryClient();
  const [initial] = useState(run);
  const [state, setState] = useState<LiveState | null>(null);
  useEffect(() => {
    const stopped = new AbortController();
    void watchRun(
      api,
      initial,
      (update) => {
        setState(update);
        if (update.mode === "complete")
          void client.invalidateQueries({ queryKey: ["run", initial.id] });
      },
      stopped.signal,
    ).catch(() => {
      if (!stopped.signal.aborted)
        setState((previous) => (previous ? { ...previous, mode: "unavailable" } : null));
    });
    return () => stopped.abort();
  }, [api, client, initial]);
  return (
    <section aria-label="Live run updates">
      <h3>Live run updates</h3>
      <p role="status">
        Event connection: {state?.mode ?? "connecting"} · last persisted sequence{" "}
        {state?.sequence ?? 0}
      </p>
      {(state?.mode === "polling" ||
        state?.mode === "unavailable" ||
        state?.mode === "reconnecting") && (
        <p className="notice">
          Live delivery is interrupted. Persisted replay and bounded polling remain available; saved
          results are unaffected.
        </p>
      )}
      {state?.progress && (
        <p>
          Committed through tick {state.progress.next_tick} · {state.progress.committed_events}{" "}
          saved events. Operational recovery does not change logical simulation time.
        </p>
      )}
      <VirtualList
        labels={(state?.events ?? []).map(
          (event) =>
            `${event.sequence} · tick ${event.t} · ${event.event.replace("REQUEST_", "")} · ${event.request_id}${event.server_id ? ` → ${event.server_id}` : ""}${event.reason ? ` · ${event.reason}` : ""}`,
        )}
        label="Recent persisted events"
      />
      {(state?.droppedFromView ?? 0) > 0 && (
        <p>
          Showing the most recent {state?.events.length} events. Use persisted playback for earlier
          pages.
        </p>
      )}
    </section>
  );
}

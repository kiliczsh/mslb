import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { useStore } from "zustand";
import type { Api } from "../../shared/api/api.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import { CaretRightIcon, PauseIcon, PlayIcon, SkipBackIcon } from "../../shared/ui/Icons.js";
import { VirtualList } from "../../shared/ui/VirtualList.js";
import { usePageVisible } from "../../shared/usePageVisible.js";
import { FlowTopology } from "./FlowTopology.js";
import { MetricNumber } from "./MetricNumber.js";
import { createPlaybackStore } from "./playbackStore.js";

export function RunPlayback({
  api,
  runId,
  terminalTick,
  autoPlay = false,
  onAutoPlayStarted,
}: {
  api: Api;
  runId: string;
  terminalTick: number;
  autoPlay?: boolean;
  onAutoPlayStarted?: (() => void) | undefined;
}) {
  const [store] = useState(() => {
    const created = createPlaybackStore(terminalTick);
    if (autoPlay && terminalTick > 0) created.getState().toggle();
    return created;
  });
  const state = useStore(store);
  const visible = usePageVisible();
  const [inspectionOpen, setInspectionOpen] = useState(false);
  const [eventPage, setEventPage] = useState<{ tick: number; cursor: string | null }>({
    tick: 0,
    cursor: null,
  });
  const cursor = eventPage.tick === state.tick ? eventPage.cursor : null;
  const selection = {
    tick: state.tick,
    server_offset: state.serverOffset,
    request_offset: state.requestOffset,
    request_kind: state.serverId ? ("active" as const) : ("pending" as const),
    server_id: state.serverId,
  };
  const frame = useQuery({
    queryKey: ["playback", runId, selection],
    queryFn: ({ signal }) => api.playback(runId, selection, signal),
    placeholderData: keepPreviousData,
  });
  const events = useQuery({
    queryKey: ["playback-events", runId, state.tick, cursor],
    queryFn: ({ signal }) => api.events(runId, state.tick, cursor, signal),
    enabled: inspectionOpen,
  });
  useEffect(() => {
    if (autoPlay) onAutoPlayStarted?.();
  }, [autoPlay, onAutoPlayStarted]);
  useEffect(() => {
    if (!state.playing || !visible) return;
    if (frame.isError) {
      store.getState().pause();
      return;
    }
    if (frame.isFetching || frame.isPlaceholderData || !frame.data) return;
    if (frame.data.terminal) {
      store.getState().pause();
      return;
    }
    const next = frame.data.frame_tick + frame.data.duration_ticks;
    const timer = setTimeout(() => store.getState().advance(next), 1000 / state.speed);
    return () => clearTimeout(timer);
  }, [
    state.playing,
    state.speed,
    frame.isFetching,
    frame.isPlaceholderData,
    frame.isError,
    frame.data,
    store,
    visible,
  ]);
  return (
    <section className="animated-playback" aria-label="Persisted playback">
      <h3>Simulation playback</h3>
      <p className="muted playback-caption">
        Animated saved results. Speed changes presentation only; idle spans jump to the next event.
      </p>
      <label>
        Playback tick
        <input
          type="range"
          min={0}
          max={terminalTick}
          step={1}
          value={state.tick}
          disabled={terminalTick === 0}
          onChange={(event) => state.seek(event.target.valueAsNumber)}
        />
      </label>
      <div className="actions playback-actions">
        <button type="button" onClick={() => state.seek(0)}>
          <SkipBackIcon size={16} aria-hidden="true" /> First tick
        </button>
        <button
          type="button"
          disabled={terminalTick === 0}
          onClick={() => {
            if (state.tick >= terminalTick) {
              state.seek(0);
              store.getState().toggle();
            } else state.toggle();
          }}
        >
          {state.playing ? (
            <PauseIcon size={18} aria-hidden="true" />
          ) : (
            <PlayIcon size={18} aria-hidden="true" />
          )}
          {state.playing
            ? "Pause playback"
            : state.tick >= terminalTick
              ? "Replay playback"
              : "Play playback"}
        </button>
        <button
          type="button"
          disabled={state.tick >= terminalTick}
          onClick={() => state.seek(state.tick + 1)}
        >
          Next tick <CaretRightIcon size={16} aria-hidden="true" />
        </button>
        <label>
          Playback speed
          <select
            value={state.speed}
            onChange={(event) => state.setSpeed(Number(event.target.value))}
          >
            {[0.5, 1, 2, 4].map((speed) => (
              <option key={speed} value={speed}>
                {speed}×
              </option>
            ))}
          </select>
        </label>
      </div>
      <p role="status">
        Selected tick {state.tick} of {terminalTick}
        {frame.isFetching ? " · loading saved interval" : ""}
        {frame.isPlaceholderData && frame.data ? ` · showing tick ${frame.data.tick}` : ""}
        {frame.data && ` · ${frame.data.active_count} active · ${frame.data.queue_depth} queued`}
      </p>
      <Feedback error={frame.error} />
      {frame.isPending && (
        <p className="playback-loading" role="status">
          Loading saved request flow…
        </p>
      )}
      {frame.data && (
        <FlowTopology
          frame={frame.data}
          playing={state.playing && visible && !frame.isFetching}
          selectedId={state.serverId}
          onInspect={state.inspect}
        />
      )}
      <details
        className="playback-inspection"
        onToggle={(event) => setInspectionOpen(event.currentTarget.open)}
      >
        <summary>Inspect requests, resources and events</summary>
        {inspectionOpen && (
          <>
            {frame.data && (
              <>
                <p>
                  {frame.data.terminal
                    ? "Terminal boundary · no resource interval"
                    : `Saved resource interval [${frame.data.frame_tick}, ${frame.data.frame_tick + frame.data.duration_ticks})`}{" "}
                  · {frame.data.active_count} active requests · {frame.data.queue_depth} queued ·{" "}
                  {frame.data.membership_count} virtual servers
                </p>
                {(frame.data.membership_added.length > 0 ||
                  frame.data.membership_removed.length > 0) && (
                  <p className="notice">
                    Membership at this interval: added{" "}
                    {frame.data.membership_added.join(", ") || "none"}, removed{" "}
                    {frame.data.membership_removed.join(", ") || "none"}.
                  </p>
                )}
                {frame.data.decisions.map((decision) => (
                  <div className="notice" key={decision.t}>
                    <strong>
                      Simulated scaling at tick {decision.t}: {decision.action.replaceAll("_", " ")}
                    </strong>
                    <p>
                      Triggers: {decision.trigger.join(", ") || "none"}.{" "}
                      {decision.applied_at === null
                        ? `Not applied: ${decision.reason ?? "none"}`
                        : `Activation boundary: ${decision.applied_at}`}
                    </p>
                    <p>
                      Added: {decision.server_ids_added.join(", ") || "none"}. Removed:{" "}
                      {decision.server_ids_removed.join(", ") || "none"}. These are logical members,
                      not Docker replicas.
                    </p>
                  </div>
                ))}
                <div className="table-scroll">
                  <table>
                    <caption>Canonical server lanes · at most 32 per view</caption>
                    <thead>
                      <tr>
                        <th scope="col">Server / capacity</th>
                        <th scope="col">Active requests</th>
                        <th scope="col">Allocated CPU</th>
                        <th scope="col">Useful CPU</th>
                        <th scope="col">Reserved MB</th>
                        <th scope="col">Action</th>
                      </tr>
                    </thead>
                    <tbody>
                      {frame.data.servers.map((server) => (
                        <tr key={server.id}>
                          <th scope="row">
                            {server.id}
                            <br />
                            <small>
                              {server.cpu_units_per_tick} CPU/tick · {server.mem_mb} MB
                            </small>
                          </th>
                          <td>{server.active_count}</td>
                          <td>
                            <MetricNumber value={server.allocated_cpu} />
                          </td>
                          <td>
                            <MetricNumber value={server.consumed_cpu} />
                          </td>
                          <td>
                            <MetricNumber value={server.reserved_mem_mb} />
                          </td>
                          <td>
                            <button type="button" onClick={() => state.inspect(server.id)}>
                              Inspect active on {server.id}
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="actions">
                  <button
                    type="button"
                    disabled={state.serverOffset === 0}
                    onClick={() => state.servers(0)}
                  >
                    First playback servers
                  </button>
                  <button
                    type="button"
                    disabled={state.serverOffset + 32 >= frame.data.membership_count}
                    onClick={() => state.servers(state.serverOffset + 32)}
                  >
                    Next playback servers
                  </button>
                  <button type="button" onClick={() => state.inspect(null)}>
                    Inspect pending queue
                  </button>
                </div>
                <h4>
                  {state.serverId
                    ? `Active requests on ${state.serverId}`
                    : "Pending queue in scheduling order"}
                </h4>
                {!frame.data.server_present && <p>This server is not a member at this tick.</p>}
                <p>
                  {frame.data.request_count} requests; showing {frame.data.request_ids.length} from
                  offset {state.requestOffset}.
                </p>
                <VirtualList
                  key={`${state.tick}:${state.serverId}:${state.requestOffset}`}
                  labels={frame.data.request_ids}
                  label="Requests at selected interval"
                />
                <div className="actions">
                  <button
                    type="button"
                    disabled={state.requestOffset === 0}
                    onClick={() => state.requests(Math.max(0, state.requestOffset - 100))}
                  >
                    Previous requests
                  </button>
                  <button
                    type="button"
                    disabled={state.requestOffset + 100 >= frame.data.request_count}
                    onClick={() => state.requests(state.requestOffset + 100)}
                  >
                    Next requests
                  </button>
                </div>
              </>
            )}
            <h4>Persisted events at selected tick</h4>
            <Feedback error={events.error} />
            <VirtualList
              key={`${state.tick}:${cursor}`}
              labels={(events.data?.items ?? []).map(
                (event) =>
                  `${event.sequence} · ${event.event.replace("REQUEST_", "")} · ${event.request_id}${event.server_id ? ` → ${event.server_id}` : ""}${event.reason ? ` · ${event.reason}` : ""}`,
              )}
              label="Events at selected tick"
            />
            <div className="actions">
              <button
                type="button"
                disabled={cursor === null}
                onClick={() => setEventPage({ tick: state.tick, cursor: null })}
              >
                <SkipBackIcon size={16} aria-hidden="true" /> First tick events
              </button>
              <button
                type="button"
                disabled={!events.data?.next_cursor}
                onClick={() =>
                  setEventPage({ tick: state.tick, cursor: events.data?.next_cursor ?? null })
                }
              >
                Next tick <CaretRightIcon size={16} aria-hidden="true" /> events
              </button>
            </div>
            <p>
              Use tick, server, request, and event controls to inspect the full saved history.
              Playback shows a bounded page rather than loading the complete trace.
            </p>
          </>
        )}
      </details>
    </section>
  );
}

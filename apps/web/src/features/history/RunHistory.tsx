import { keepPreviousData, useMutation, useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { useDraftState, useDraftStorage } from "../../app/workspaceDrafts.js";
import type { Api } from "../../shared/api/api.js";
import { pageSchema, terminal } from "../../shared/api/contracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import {
  ArrowClockwiseIcon,
  CopyIcon,
  EyeIcon,
  MagnifyingGlassIcon,
  PlayIcon,
} from "../../shared/ui/Icons.js";
import { StatusIcon } from "../../shared/ui/StatusIcon.js";
import { failureMessage, type HistoryEntry, historyEntrySchema } from "./historyContracts.js";
import { createHistoryDraft } from "./historyDraft.js";

export function RunHistory({
  api,
  kind,
  onOpen,
  onRepeat,
}: {
  api: Api;
  kind: "logical" | "docker";
  onOpen: (options: { id: string; playback: boolean }) => void;
  onRepeat: (entry: HistoryEntry) => Promise<void>;
}) {
  const storage = useDraftStorage();
  const [savedDraft, setSavedDraft] = useDraftState({
    storage,
    name: kind === "logical" ? "logicalHistory" : "dockerHistory",
  });
  const [initialDraft] = useState(createHistoryDraft);
  const draft = savedDraft ?? initialDraft;
  const setDraft: typeof setSavedDraft = (next) =>
    setSavedDraft((current) => (typeof next === "function" ? next(current ?? initialDraft) : next));
  const [search, setSearch] = useState(draft.search);
  const restored = useRef(false);
  const query = new URLSearchParams({ limit: "20" });
  for (const key of ["search", "status", "strategy", "mode", "cursor"] as const)
    if (draft[key]) query.set(key, draft[key]);
  const history = useQuery({
    queryKey: ["browser-history", kind, query.toString()],
    queryFn: ({ signal }) =>
      api.request(`history/${kind}?${query}`, pageSchema(historyEntrySchema), { signal }),
    placeholderData: keepPreviousData,
    refetchInterval: 3000,
  });
  const active = useQuery({
    queryKey: ["browser-history-active", kind],
    queryFn: ({ signal }) =>
      api.request(`history/${kind}?status=active&limit=8`, pageSchema(historyEntrySchema), {
        signal,
      }),
    enabled: draft.status !== "active",
    refetchInterval: 3000,
  });
  const repeat = useMutation({ mutationFn: onRepeat });
  useEffect(() => {
    if (history.data && !restored.current) {
      restored.current = true;
      if (window.scrollY !== draft.scrollTop)
        window.scrollTo({ top: draft.scrollTop, behavior: "auto" });
    }
  }, [history.data, draft.scrollTop]);
  function filter(next: Partial<Pick<typeof draft, "search" | "status" | "strategy" | "mode">>) {
    setDraft((current) => ({ ...current, ...next, cursor: null, previous: [], scrollTop: 0 }));
  }
  function remember() {
    setDraft((current) => ({ ...current, scrollTop: window.scrollY }));
  }
  function open({ id, playback = false }: { id: string; playback?: boolean }) {
    remember();
    onOpen({ id, playback });
  }
  return (
    <section
      className="run-history-workspace"
      aria-label={kind === "logical" ? "Simulation history" : "Docker history"}
    >
      <div className="history-heading">
        <h2>Run history</h2>
        <span className="muted">Newest first</span>
        <button
          type="button"
          onClick={() => void history.refetch()}
          disabled={history.isFetching}
          aria-label="Refresh history"
          title="Refresh history"
        >
          <ArrowClockwiseIcon size={16} aria-hidden="true" />
        </button>
      </div>
      {draft.status !== "active" && active.data && active.data.items.length > 0 && (
        <section className="history-active" aria-label="Active runs">
          <strong>Running now</strong>
          {active.data.items.map((run) => (
            <div className="history-active-row" key={run.id}>
              <StatusIcon status={run.status} />
              <span>
                <strong>{run.source_filename}</strong> · {run.request_count.toLocaleString()}{" "}
                requests · {run.status.replaceAll("_", " ")}
                {run.progress && (
                  <small className="history-meta">
                    Saved through tick {run.progress.next_tick.toLocaleString()} ·{" "}
                    {run.progress.committed_events.toLocaleString()} events
                  </small>
                )}
              </span>
              <button type="button" onClick={() => open({ id: run.id })}>
                Watch <EyeIcon size={16} aria-hidden="true" />
              </button>
            </div>
          ))}
          {active.data.next_cursor && (
            <button type="button" onClick={() => filter({ status: "active" })}>
              View all active runs
            </button>
          )}
        </section>
      )}
      <form
        className="history-filters"
        onSubmit={(event) => {
          event.preventDefault();
          filter({ search: search.trim() });
        }}
      >
        <label className="history-search">
          <span className="sr-only">Search history</span>
          <input
            type="search"
            value={search}
            maxLength={128}
            placeholder="Dataset filename or run ID"
            onChange={(event) => {
              setSearch(event.target.value);
              if (!event.target.value) filter({ search: "" });
            }}
          />
        </label>
        <button type="submit">
          <MagnifyingGlassIcon size={16} aria-hidden="true" /> Search
        </button>
        <label>
          Status
          <select value={draft.status} onChange={(event) => filter({ status: event.target.value })}>
            <option value="">All statuses</option>
            {["active", "queued", "running", "completed", "failed", "cancelled"].map((status) => (
              <option key={status} value={status}>
                {status[0]?.toUpperCase()}
                {status.slice(1)}
              </option>
            ))}
          </select>
        </label>
        <details className="history-more-filters">
          <summary>More filters</summary>
          <label>
            Strategy
            <select
              value={draft.strategy}
              onChange={(event) => filter({ strategy: event.target.value })}
            >
              <option value="">All strategies</option>
              {["first_fit", "round_robin", "least_connections", "most_available_memory"].map(
                (strategy) => (
                  <option key={strategy} value={strategy}>
                    {strategy.replaceAll("_", " ")}
                  </option>
                ),
              )}
            </select>
          </label>
          {kind === "logical" && (
            <label>
              Mode
              <select value={draft.mode} onChange={(event) => filter({ mode: event.target.value })}>
                <option value="">All modes</option>
                <option value="sequential">Sequential</option>
                <option value="concurrent">Concurrent</option>
              </select>
            </label>
          )}
        </details>
      </form>
      <Feedback error={history.error ?? repeat.error} />
      {history.isPending && <p role="status">Loading run history…</p>}
      {history.data?.items.length === 0 && <p>No runs match these filters.</p>}
      {history.data && history.data.items.length > 0 && (
        <section className="table-scroll history-table" aria-label="Run history table">
          <table>
            <thead>
              <tr>
                <th scope="col">Dataset / requests</th>
                <th scope="col">Strategy / mode</th>
                <th scope="col">Status</th>
                <th scope="col">Started</th>
                <th scope="col">Actions</th>
              </tr>
            </thead>
            <tbody>
              {history.data.items.map((run) => (
                <tr key={run.id}>
                  <th scope="row">
                    <span>{run.source_filename}</span>
                    <small className="history-meta">
                      {run.request_count.toLocaleString()} requests ·{" "}
                      <span title={run.id}>{run.id.slice(0, 8)}</span>
                    </small>
                  </th>
                  <td>
                    {run.strategy.replaceAll("_", " ")}
                    <small className="history-meta">
                      {run.execution_mode === "docker" ? "Real HTTP" : run.execution_mode}
                    </small>
                  </td>
                  <td>
                    <span className="icon-label">
                      <StatusIcon status={run.status} />
                      {run.status.replaceAll("_", " ")}
                    </span>
                    {run.error_code && (
                      <small className="history-error" title={run.error_code}>
                        {failureMessage(run.error_code)}
                      </small>
                    )}
                  </td>
                  <td>
                    <time
                      dateTime={run.created_at}
                      title={new Date(run.created_at).toLocaleString()}
                    >
                      {new Date(run.created_at).toLocaleTimeString([], {
                        hour: "2-digit",
                        minute: "2-digit",
                      })}
                      <small className="history-meta">
                        {new Date(run.created_at).toLocaleDateString()}
                      </small>
                    </time>
                  </td>
                  <td>
                    <div className="history-row-actions">
                      <button
                        type="button"
                        onClick={() => open({ id: run.id })}
                        aria-label={`View ${run.id}`}
                        title="View run"
                      >
                        <EyeIcon size={16} aria-hidden="true" />
                        {terminal(run.status) ? "View" : "Watch"}
                      </button>
                      {kind === "logical" && run.status === "completed" && (
                        <button
                          type="button"
                          onClick={() => open({ id: run.id, playback: true })}
                          aria-label={`Replay ${run.id}`}
                          title="Replay"
                        >
                          <PlayIcon size={16} aria-hidden="true" />
                        </button>
                      )}
                      <button
                        type="button"
                        disabled={repeat.isPending}
                        onClick={() => {
                          remember();
                          repeat.mutate(run);
                        }}
                        aria-label={`Run again ${run.id}`}
                        title="Run again with this dataset and settings"
                      >
                        <CopyIcon size={16} aria-hidden="true" />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
      {(draft.cursor || history.data?.next_cursor) && (
        <div className="actions history-pagination">
          <button
            type="button"
            disabled={!draft.cursor || history.isPlaceholderData}
            onClick={() => filter({})}
          >
            Newest
          </button>
          <button
            type="button"
            disabled={draft.previous.length === 0 || history.isPlaceholderData}
            onClick={() =>
              setDraft((current) => ({
                ...current,
                cursor: current.previous.at(-1) ?? null,
                previous: current.previous.slice(0, -1),
              }))
            }
          >
            Previous
          </button>
          <button
            type="button"
            disabled={!history.data?.next_cursor || history.isPlaceholderData}
            onClick={() => {
              const cursor = history.data?.next_cursor;
              if (cursor)
                setDraft((current) => ({
                  ...current,
                  previous: [...current.previous, current.cursor].slice(-100),
                  cursor,
                }));
            }}
          >
            Older runs
          </button>
        </div>
      )}
    </section>
  );
}

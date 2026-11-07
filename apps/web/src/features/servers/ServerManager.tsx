import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { z } from "zod";
import { emptyServer, useDraftState, useDraftStorage } from "../../app/workspaceDrafts.js";
import { type Api, json } from "../../shared/api/api.js";
import { type Server, type ServerInput, serverInputSchema } from "../../shared/api/contracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import {
  ArrowClockwiseIcon,
  CheckIcon,
  PencilSimpleIcon,
  PlusIcon,
  TrashIcon,
  XIcon,
} from "../../shared/ui/Icons.js";
import { ServerPresets } from "./ServerPresets.js";

type SaveRequest = {
  body: ServerInput;
  expectedVersion: number | null;
  editSession: number;
};

export function ServerManager({ api }: { api: Api }) {
  const client = useQueryClient();
  const servers = useQuery({ queryKey: ["servers"], queryFn: ({ signal }) => api.servers(signal) });
  const storage = useDraftStorage();
  const [editing, setEditing] = useDraftState({ storage, name: "editingServer" });
  const [draft, setDraft] = useDraftState({ storage, name: "serverDraft" });
  const [presetBusy, setPresetBusy] = useState(false);
  const [deleting, setDeleting] = useState<Server | null>(null);
  const keepServer = useRef<HTMLButtonElement>(null);
  const deleteTrigger = useRef<HTMLButtonElement | null>(null);
  const refreshButton = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    if (deleting) keepServer.current?.focus();
  }, [deleting]);
  const [localError, setLocalError] = useState("");
  const save = useMutation({
    mutationFn: ({ body, expectedVersion }: SaveRequest) =>
      expectedVersion === null ? api.createServer(body) : api.updateServer(body, expectedVersion),
    onSuccess: async (_saved, request) => {
      if (storage.current.serverSession === request.editSession) {
        storage.current.serverSession += 1;
        setDraft(emptyServer);
        setEditing(null);
      }
      await client.invalidateQueries({ queryKey: ["servers"] });
    },
  });
  const remove = useMutation({
    mutationFn: (server: Server) =>
      api.request(`servers/${encodeURIComponent(server.id)}`, z.undefined(), {
        ...json(undefined, "DELETE"),
        headers: { "If-Match": `"${server.version}"` },
      }),
    onSuccess: async () => {
      setDeleting(null);
      await client.invalidateQueries({ queryKey: ["servers"] });
      refreshButton.current?.focus();
    },
  });
  function edit(server: Server | null) {
    storage.current.serverSession += 1;
    setEditing(server);
    setDraft(
      server
        ? serverInputSchema.parse({
            id: server.id,
            cpu_units_per_tick: server.cpu_units_per_tick,
            mem_mb: server.mem_mb,
            rate_limit_per_sec: server.rate_limit_per_sec,
          })
        : emptyServer,
    );
    setLocalError("");
    save.reset();
  }
  return (
    <section aria-label="Servers" className="servers-workspace">
      <div className="server-toolbar">
        <p className="eyebrow server-toolbar-label">Servers</p>
        <div className="actions">
          <button
            type="button"
            className="primary"
            disabled={presetBusy}
            onClick={() => edit(null)}
          >
            <PlusIcon size={18} aria-hidden="true" />
            Add server
          </button>
          <button
            ref={refreshButton}
            type="button"
            onClick={() => {
              void servers.refetch();
            }}
          >
            <ArrowClockwiseIcon size={18} aria-hidden="true" />
            Refresh servers
          </button>
        </div>
      </div>
      <ServerPresets
        api={api}
        servers={servers.data?.items}
        disabled={save.isPending || remove.isPending}
        onBusyChange={setPresetBusy}
        onApplied={() => {
          edit(null);
          setDeleting(null);
        }}
      />
      {servers.data && (
        <dl className="capacity-summary">
          <div>
            <dt>Servers</dt>
            <dd>{servers.data.items.length}</dd>
          </div>
          <div>
            <dt>Total CPU</dt>
            <dd>
              {servers.data.items
                .reduce((sum, server) => sum + server.cpu_units_per_tick, 0)
                .toLocaleString()}{" "}
              <small>units/tick</small>
            </dd>
          </div>
          <div>
            <dt>Total memory</dt>
            <dd>
              {servers.data.items.reduce((sum, server) => sum + server.mem_mb, 0).toLocaleString()}{" "}
              <small>MB</small>
            </dd>
          </div>
        </dl>
      )}
      <p className="muted">Changes apply to future runs. Saved snapshots stay immutable.</p>
      <Feedback error={servers.error} />
      <div className="server-layout">
        <div className="server-table">
          {servers.isPending ? (
            <p role="status">Loading servers…</p>
          ) : (
            <>
              {servers.data?.items.length === 0 && (
                <p className="notice">No servers configured. Add a server to enable simulations.</p>
              )}
              <section
                className="table-scroll"
                aria-label="Server capacities; scroll horizontally on small screens"
              >
                <table>
                  <caption>Current server capacities</caption>
                  <thead>
                    <tr>
                      <th scope="col">Server</th>
                      <th scope="col">CPU units/tick</th>
                      <th scope="col">Memory (MB)</th>
                      <th scope="col">Starts/tick</th>
                      <th scope="col">Version</th>
                      <th scope="col">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {servers.data?.items.map((server) => (
                      <tr
                        key={server.id}
                        className={editing?.id === server.id ? "selected-row" : undefined}
                      >
                        <th scope="row">{server.id}</th>
                        <td>{server.cpu_units_per_tick}</td>
                        <td>{server.mem_mb}</td>
                        <td>{server.rate_limit_per_sec}</td>
                        <td>{server.version}</td>
                        <td className="actions">
                          <button type="button" disabled={presetBusy} onClick={() => edit(server)}>
                            <PencilSimpleIcon size={16} aria-hidden="true" />
                            Edit {server.id}
                          </button>
                          <button
                            type="button"
                            disabled={remove.isPending || presetBusy}
                            onClick={(event) => {
                              deleteTrigger.current = event.currentTarget;
                              remove.reset();
                              setDeleting(server);
                            }}
                          >
                            <TrashIcon size={16} aria-hidden="true" />
                            Delete {server.id}
                          </button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </section>
            </>
          )}
          <p className="muted">Capacity is captured when a run is queued.</p>
        </div>
        <form
          className="server-editor"
          inert={presetBusy}
          onSubmit={(event) => {
            event.preventDefault();
            const parsed = serverInputSchema.safeParse(draft);
            if (!parsed.success) {
              setLocalError("Use a server ID and integer capacities from 1 to 1,000,000,000.");
              return;
            }
            setLocalError("");
            storage.current.serverSession += 1;
            save.mutate({
              body: parsed.data,
              expectedVersion: editing?.version ?? null,
              editSession: storage.current.serverSession,
            });
          }}
        >
          <h3>{editing ? `Edit ${editing.id} · version ${editing.version}` : "Add a server"}</h3>
          <div className="form-grid">
            <label>
              Server ID
              <input
                required
                maxLength={128}
                disabled={editing !== null || save.isPending}
                value={draft.id}
                onChange={(event) => setDraft({ ...draft, id: event.target.value })}
              />
            </label>
            {(["cpu_units_per_tick", "mem_mb", "rate_limit_per_sec"] as const).map((key) => (
              <label key={key}>
                {
                  {
                    cpu_units_per_tick: "CPU units per tick",
                    mem_mb: "Memory capacity (MB)",
                    rate_limit_per_sec: "Request starts per tick",
                  }[key]
                }
                <input
                  type="number"
                  disabled={save.isPending}
                  required
                  min={1}
                  max={1_000_000_000}
                  step={1}
                  value={Number.isFinite(draft[key]) ? draft[key] : ""}
                  onChange={(event) => setDraft({ ...draft, [key]: event.target.valueAsNumber })}
                />
              </label>
            ))}
          </div>
          {localError && <p role="alert">{localError}</p>}
          <Feedback error={save.error} />
          {save.isError && editing && (
            <div className="notice">
              <p>
                Your input is preserved. Refresh to inspect the current version, then deliberately
                reload before editing again.
              </p>
              <button
                type="button"
                onClick={async () => {
                  const currentSession = storage.current.serverSession;
                  const result = await servers.refetch();
                  if (currentSession !== storage.current.serverSession) return;
                  const current = result.data?.items.find((server) => server.id === editing.id);
                  if (current) edit(current);
                  else
                    setLocalError(
                      "This server no longer exists. Cancel editing to create a new server.",
                    );
                }}
              >
                Reload current server
              </button>
            </div>
          )}
          <div className="actions">
            <button className="primary" disabled={save.isPending} type="submit">
              <CheckIcon size={18} aria-hidden="true" />
              {save.isPending ? "Saving…" : editing ? "Save changes" : "Create server"}
            </button>
            {editing && (
              <button type="button" onClick={() => edit(null)}>
                <XIcon size={18} aria-hidden="true" />
                Cancel editing
              </button>
            )}
          </div>
        </form>
      </div>
      {deleting && (
        <div
          role="alertdialog"
          aria-labelledby="delete-heading"
          aria-describedby="delete-description"
          className="panel danger"
        >
          <h3 id="delete-heading">Delete {deleting.id}?</h3>
          <p id="delete-description">
            This removes the server from future configurations. Existing run snapshots are
            preserved.
          </p>
          <Feedback error={remove.error} />
          <div className="actions">
            <button
              type="button"
              disabled={remove.isPending || presetBusy}
              onClick={() => remove.mutate(deleting)}
            >
              Confirm deletion
            </button>
            <button
              type="button"
              ref={keepServer}
              disabled={remove.isPending || presetBusy}
              onClick={() => {
                setDeleting(null);
                deleteTrigger.current?.focus();
              }}
            >
              Keep server
            </button>
          </div>
        </div>
      )}
    </section>
  );
}

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { z } from "zod";
import { type Api, json } from "../lib/api.js";
import { type Server, type ServerInput, serverInputSchema } from "../lib/contracts.js";
import { Feedback } from "./Feedback.js";

const emptyServer: ServerInput = {
  id: "",
  cpu_units_per_tick: 1,
  mem_mb: 1024,
  rate_limit_per_sec: 10,
};

export function ServerManager({ api }: { api: Api }) {
  const client = useQueryClient();
  const servers = useQuery({ queryKey: ["servers"], queryFn: ({ signal }) => api.servers(signal) });
  const [editing, setEditing] = useState<Server | null>(null);
  const [draft, setDraft] = useState<ServerInput>(emptyServer);
  const [deleting, setDeleting] = useState<Server | null>(null);
  const keepServer = useRef<HTMLButtonElement>(null);
  useEffect(() => {
    if (deleting) keepServer.current?.focus();
  }, [deleting]);
  const [localError, setLocalError] = useState("");
  const save = useMutation({
    mutationFn: (body: ServerInput) =>
      editing ? api.updateServer(body, editing.version) : api.createServer(body),
    onSuccess: async () => {
      setDraft(emptyServer);
      setEditing(null);
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
    },
  });
  function edit(server: Server | null) {
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
    <section aria-labelledby="servers-heading">
      <div className="section-heading">
        <div>
          <h2 id="servers-heading">Server configuration</h2>
          <p>Changes apply to future runs. Saved snapshots stay immutable.</p>
        </div>
        <button
          type="button"
          onClick={() => {
            void servers.refetch();
          }}
        >
          Refresh servers
        </button>
      </div>
      <Feedback error={servers.error} />
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
                  <tr key={server.id}>
                    <th scope="row">{server.id}</th>
                    <td>{server.cpu_units_per_tick}</td>
                    <td>{server.mem_mb}</td>
                    <td>{server.rate_limit_per_sec}</td>
                    <td>{server.version}</td>
                    <td className="actions">
                      <button type="button" onClick={() => edit(server)}>
                        Edit {server.id}
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          remove.reset();
                          setDeleting(server);
                        }}
                      >
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
      <form
        className="panel"
        onSubmit={(event) => {
          event.preventDefault();
          const parsed = serverInputSchema.safeParse(draft);
          if (!parsed.success) {
            setLocalError("Use a server ID and integer capacities from 1 to 1,000,000,000.");
            return;
          }
          setLocalError("");
          save.mutate(parsed.data);
        }}
      >
        <h3>{editing ? `Edit ${editing.id} · version ${editing.version}` : "Add a server"}</h3>
        <div className="form-grid">
          <label>
            Server ID
            <input
              required
              maxLength={128}
              disabled={editing !== null}
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
                const result = await servers.refetch();
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
            {save.isPending ? "Saving…" : editing ? "Save changes" : "Create server"}
          </button>
          {editing && (
            <button type="button" onClick={() => edit(null)}>
              Cancel editing
            </button>
          )}
        </div>
      </form>
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
              disabled={remove.isPending}
              onClick={() => remove.mutate(deleting)}
            >
              Confirm deletion
            </button>
            <button type="button" ref={keepServer} onClick={() => setDeleting(null)}>
              Keep server
            </button>
          </div>
        </div>
      )}
    </section>
  );
}

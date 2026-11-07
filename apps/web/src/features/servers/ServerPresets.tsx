import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import type { Api } from "../../shared/api/api.js";
import { json } from "../../shared/api/api.js";
import {
  pageSchema,
  type Server,
  type ServerInput,
  serverSchema,
} from "../../shared/api/contracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import { CheckIcon, EyeIcon, SlidersHorizontalIcon, XIcon } from "../../shared/ui/Icons.js";
import { type ClusterProfile, clusterProfiles, presetServers } from "./clusterPresets.js";

export function ServerPresets({
  api,
  servers,
  disabled,
  onApplied,
  onBusyChange,
}: {
  api: Api;
  servers: readonly Server[] | undefined;
  disabled: boolean;
  onApplied: () => void;
  onBusyChange: (busy: boolean) => void;
}) {
  const client = useQueryClient();
  const [count, setCount] = useState(3);
  const [profile, setProfile] = useState<ClusterProfile>("balanced");
  const [review, setReview] = useState<{
    expected: readonly Server[];
    servers: ServerInput[];
  } | null>(null);
  const selected = clusterProfiles.find((item) => item.id === profile) ?? clusterProfiles[0];
  const apply = useMutation({
    mutationFn: (body: NonNullable<typeof review>) =>
      api.request("servers", pageSchema(serverSchema), json(body, "PUT")),
    onMutate: () => onBusyChange(true),
    onSuccess: (saved) => {
      client.setQueryData(["servers"], saved);
      setReview(null);
      onApplied();
    },
    onSettled: async () => {
      await client.invalidateQueries({ queryKey: ["servers"] });
      onBusyChange(false);
    },
  });
  const busy = disabled || apply.isPending;
  return (
    <section className="server-presets" aria-label="Cluster presets">
      <div className="preset-controls">
        <label>
          Server count
          <select
            value={count}
            disabled={busy}
            onChange={(event) => {
              setCount(Number(event.target.value));
              setReview(null);
              apply.reset();
            }}
          >
            {[3, 4, 5].map((value) => (
              <option value={value} key={value}>
                {value} servers
              </option>
            ))}
          </select>
        </label>
        <label>
          Cluster profile
          <select
            value={profile}
            disabled={busy}
            onChange={(event) => {
              const value = clusterProfiles.find((item) => item.id === event.target.value);
              if (value) setProfile(value.id);
              setReview(null);
              apply.reset();
            }}
          >
            {clusterProfiles.map((item) => (
              <option value={item.id} key={item.id}>
                {item.label}
              </option>
            ))}
          </select>
        </label>
        <button
          type="button"
          disabled={busy || !servers}
          onClick={() => {
            if (servers)
              setReview({ expected: servers, servers: presetServers({ count, profile }) });
            apply.reset();
          }}
        >
          <EyeIcon size={16} aria-hidden="true" /> Preview configuration
        </button>
      </div>
      <p className="muted preset-description">
        <SlidersHorizontalIcon size={16} aria-hidden="true" /> {selected.description}
      </p>
      {review && (
        <div className="preset-review">
          <p>
            Replace the current {review.expected.length}-server cluster with these{" "}
            {review.servers.length} servers. Saved runs keep their snapshots.
          </p>
          <section className="table-scroll" aria-label="Preset server preview">
            <table>
              <caption>{selected.label} · configuration to apply</caption>
              <thead>
                <tr>
                  <th scope="col">Server</th>
                  <th scope="col">CPU / tick</th>
                  <th scope="col">Memory MB</th>
                  <th scope="col">Starts / tick</th>
                </tr>
              </thead>
              <tbody>
                {review.servers.map((server) => (
                  <tr key={server.id}>
                    <th scope="row">{server.id}</th>
                    <td>{server.cpu_units_per_tick}</td>
                    <td>{server.mem_mb}</td>
                    <td>{server.rate_limit_per_sec}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
          <div className="actions">
            <button
              type="button"
              className="primary"
              disabled={busy}
              onClick={() => apply.mutate(review)}
            >
              <CheckIcon size={16} aria-hidden="true" />
              {apply.isPending ? "Applying configuration…" : "Apply configuration"}
            </button>
            <button
              type="button"
              disabled={busy}
              onClick={() => {
                setReview(null);
                apply.reset();
              }}
            >
              <XIcon size={16} aria-hidden="true" />
              Cancel
            </button>
          </div>
        </div>
      )}
      <Feedback error={apply.error} />
      {apply.isError && (
        <p className="muted">
          Refresh servers and preview again before retrying. No partial configuration was saved.
        </p>
      )}
      {apply.isSuccess && (
        <p role="status">
          Configuration applied: {count} servers · {selected.label}.
        </p>
      )}
    </section>
  );
}

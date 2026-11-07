import type { Api } from "../../shared/api/api.js";
import { Help } from "../../shared/ui/Help.js";
import { HardDrivesIcon } from "../../shared/ui/Icons.js";

/** Show the capacities that will be frozen when a run is queued. */
export function ClusterPreview({
  servers,
  pending,
}: {
  servers: Awaited<ReturnType<Api["servers"]>> | undefined;
  pending: boolean;
}) {
  return (
    <aside className="simulation-cluster" aria-label="Current cluster">
      <div className="simulation-cluster-heading">
        <h3 className="simulation-section-title">
          <HardDrivesIcon size={18} aria-hidden="true" /> Current cluster
        </h3>
        <Help text="Each run saves its inputs. Server edits affect future runs." />
      </div>
      {pending && <p role="status">Loading servers…</p>}
      {servers && (
        <>
          <p>
            {servers.items.length} servers ·{" "}
            {servers.items
              .reduce((sum, server) => sum + server.cpu_units_per_tick, 0)
              .toLocaleString()}{" "}
            CPU units/tick ·{" "}
            {servers.items.reduce((sum, server) => sum + server.mem_mb, 0).toLocaleString()} MB
          </p>
          <ul className="cluster-preview">
            {servers.items.map((server) => (
              <li key={server.id}>
                <span className="mono">{server.id}</span>
                <span>
                  {server.cpu_units_per_tick} CPU · {server.mem_mb.toLocaleString()} MB ·{" "}
                  {server.rate_limit_per_sec} starts/tick
                </span>
              </li>
            ))}
          </ul>
        </>
      )}
      {servers?.items.length === 0 && (
        <p className="notice">Add a server in Servers before starting a simulation.</p>
      )}
      <p className="muted simulation-cluster-note">Logical time in ticks. No real HTTP traffic.</p>
      <p className="muted simulation-cluster-note">
        The run saves the latest server capacities when queued.
      </p>
    </aside>
  );
}

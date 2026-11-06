import { QueryClientProvider, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { DatasetManager } from "./components/DatasetManager.js";
import { Feedback } from "./components/Feedback.js";
import { DatabaseIcon, GraphIcon, HardDrivesIcon, UserCircleIcon } from "./components/Icons.js";
import { Ribbon } from "./components/Ribbon.js";
import { RunWorkspace } from "./components/RunWorkspace.js";
import { ServerManager } from "./components/ServerManager.js";
import { Api } from "./lib/api.js";
import type { Dataset } from "./lib/contracts.js";
import { createQueryClient } from "./lib/queryClient.js";
import { WorkspaceDraftProvider } from "./lib/workspaceDrafts.js";

function Workspace({ api, connect }: { api: Api; connect: (token: string) => void }) {
  const [tab, setTab] = useState("servers");
  const [dataset, setDataset] = useState<Dataset | null>(null);
  const ready = useQuery({
    queryKey: ["readiness"],
    queryFn: ({ signal }) => api.ready(signal),
    refetchInterval: 10_000,
  });
  return (
    <WorkspaceDraftProvider>
      <a className="skip-link" href="#workspace">
        Skip to workspace
      </a>
      <header className="app-header">
        <a className="brand" href="#workspace" aria-label="mslb workspace">
          <Ribbon compact />
          <span>mslb</span>
          <span className="brand-description">Load Balancer</span>
        </a>
        <nav aria-label="Workspace" className="workspace-nav">
          {[
            { id: "servers", label: "Servers", Icon: HardDrivesIcon },
            { id: "datasets", label: "Datasets", Icon: DatabaseIcon },
            { id: "runs", label: "Simulations", Icon: GraphIcon },
          ].map(({ id, label, Icon }) => (
            <button
              key={id}
              type="button"
              aria-current={tab === id ? "page" : undefined}
              onClick={() => setTab(id)}
            >
              <Icon size={18} aria-hidden="true" />
              {label}
            </button>
          ))}
        </nav>
        <details className="session-menu">
          <summary>
            <UserCircleIcon size={20} aria-hidden="true" />
            Connection
          </summary>
          <form
            className="connection"
            onSubmit={(event) => {
              event.preventDefault();
              const form = event.currentTarget;
              const value = new FormData(form).get("token");
              connect(typeof value === "string" ? value : "");
              form.reset();
            }}
          >
            <label>
              Bearer token (authenticated mode)
              <input
                name="token"
                type="password"
                autoComplete="off"
                minLength={32}
                maxLength={256}
              />
            </label>
            <div className="actions">
              <button type="submit" className="primary">
                Connect
              </button>
              <button type="button" onClick={() => connect("")}>
                Clear session
              </button>
            </div>
            <small>
              Leave blank for local demo. Credentials stay in memory and clear on reload.
            </small>
          </form>
        </details>
      </header>
      <main id="workspace" tabIndex={-1}>
        <h1 className="sr-only">Simulation workspace</h1>
        <Feedback error={ready.error} />
        {ready.data && !ready.data.complete_product && (
          <details className="readiness">
            <summary>Product readiness: incomplete</summary>
            <p>
              Logical API: {ready.data.api_ready ? "available" : "unavailable"}. Additional
              capabilities are being integrated.
            </p>
            <ul>
              {Object.entries(ready.data.capabilities).map(([name, status]) => (
                <li key={name}>
                  {name.replaceAll("_", " ")}: {status.replaceAll("_", " ")}
                </li>
              ))}
            </ul>
          </details>
        )}
        {tab === "servers" && <ServerManager api={api} />}
        {tab === "datasets" && (
          <DatasetManager api={api} selected={dataset} onSelect={setDataset} />
        )}
        {tab === "runs" && (
          <RunWorkspace api={api} dataset={dataset} onChooseDataset={() => setTab("datasets")} />
        )}
      </main>
      <footer>
        <span>mslb / Simulation workspace</span>
        <span>Logical time. Exact saved results.</span>
      </footer>
    </WorkspaceDraftProvider>
  );
}

export function App() {
  const [session, setSession] = useState(() => ({
    api: new Api(),
    client: createQueryClient(),
    version: 0,
  }));
  function connect(token: string) {
    session.api.close();
    session.client.clear();
    setSession({ api: new Api(token), client: createQueryClient(), version: session.version + 1 });
  }
  return (
    <QueryClientProvider client={session.client} key={session.version}>
      <Workspace api={session.api} connect={connect} />
    </QueryClientProvider>
  );
}

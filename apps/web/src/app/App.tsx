import { QueryClientProvider, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { DatasetManager } from "../features/datasets/DatasetManager.js";
import { Home } from "../features/demo/Home.js";
import { LiveDemo } from "../features/demo/LiveDemo.js";
import { ServerManager } from "../features/servers/ServerManager.js";
import { RunWorkspace } from "../features/simulations/RunWorkspace.js";
import { Api } from "../shared/api/api.js";
import type { Dataset } from "../shared/api/contracts.js";
import { createQueryClient } from "../shared/queryClient.js";
import { Feedback } from "../shared/ui/Feedback.js";
import { DatabaseIcon, GraphIcon, HardDrivesIcon, UserCircleIcon } from "../shared/ui/Icons.js";
import { Ribbon } from "../shared/ui/Ribbon.js";
import { useDraftState, useDraftStorage, WorkspaceDraftProvider } from "./workspaceDrafts.js";

function OpenDemo({
  api,
  onServers,
  onRuns,
}: {
  api: Api;
  onServers: () => void;
  onRuns: () => void;
}) {
  const storage = useDraftStorage();
  const [, setSelected] = useDraftState({ storage, name: "selectedRun" });
  return (
    <LiveDemo
      api={api}
      onServers={onServers}
      onOpenRun={(id) => {
        setSelected(id);
        onRuns();
      }}
    />
  );
}

function Workspace({ api, connect }: { api: Api; connect: (token: string) => void }) {
  const [tab, setTab] = useState("home");
  function navigate(next: string) {
    setTab(next);
    queueMicrotask(() => document.getElementById("workspace")?.focus());
  }
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
        <button
          type="button"
          className="brand"
          aria-label="mslb home"
          onClick={() => navigate("home")}
        >
          <Ribbon compact />
          <span>mslb</span>
          <span className="brand-description">Load Balancer</span>
        </button>
        <nav aria-label="Workspace" className="workspace-nav">
          {[
            { id: "home", label: "Home", Icon: GraphIcon },
            { id: "demo", label: "Guided demo", Icon: GraphIcon },
            { id: "servers", label: "Servers", Icon: HardDrivesIcon },
            { id: "datasets", label: "Datasets", Icon: DatabaseIcon },
            { id: "runs", label: "Simulations", Icon: GraphIcon },
          ].map(({ id, label, Icon }) => (
            <button
              key={id}
              type="button"
              aria-current={tab === id ? "page" : undefined}
              onClick={() => navigate(id)}
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
        {tab === "home" && (
          <Home onDemo={() => navigate("demo")} onServers={() => navigate("servers")} />
        )}
        {tab === "demo" && (
          <OpenDemo
            api={api}
            onServers={() => navigate("servers")}
            onRuns={() => navigate("runs")}
          />
        )}
        {tab === "servers" && <ServerManager api={api} />}
        {tab === "datasets" && (
          <DatasetManager api={api} selected={dataset} onSelect={setDataset} />
        )}
        {tab === "runs" && (
          <RunWorkspace api={api} dataset={dataset} onChooseDataset={() => navigate("datasets")} />
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

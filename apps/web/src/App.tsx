import { QueryClientProvider, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { DatasetManager } from "./components/DatasetManager.js";
import { Feedback } from "./components/Feedback.js";
import { RunWorkspace } from "./components/RunWorkspace.js";
import { ServerManager } from "./components/ServerManager.js";
import { Api } from "./lib/api.js";
import type { Dataset } from "./lib/contracts.js";
import { createQueryClient } from "./lib/queryClient.js";

function Workspace({ api }: { api: Api }) {
  const [tab, setTab] = useState("servers");
  const [dataset, setDataset] = useState<Dataset | null>(null);
  const ready = useQuery({
    queryKey: ["readiness"],
    queryFn: ({ signal }) => api.ready(signal),
    refetchInterval: 10_000,
  });
  return (
    <>
      <nav aria-label="Workspace">
        <button
          type="button"
          aria-current={tab === "servers" ? "page" : undefined}
          onClick={() => setTab("servers")}
        >
          Servers
        </button>
        <button
          type="button"
          aria-current={tab === "datasets" ? "page" : undefined}
          onClick={() => setTab("datasets")}
        >
          Datasets
        </button>
        <button
          type="button"
          aria-current={tab === "runs" ? "page" : undefined}
          onClick={() => setTab("runs")}
        >
          Simulations
        </button>
      </nav>
      <main id="workspace">
        <Feedback error={ready.error} />
        {ready.data && !ready.data.complete_product && (
          <details className="notice">
            <summary>Product readiness: incomplete</summary>
            <p>
              Logical API: {ready.data.api_ready ? "available" : "unavailable"}. Other selected
              capabilities are still being integrated.
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
        {tab === "runs" && <RunWorkspace api={api} dataset={dataset} />}
      </main>
    </>
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
    <>
      <a className="skip-link" href="#workspace">
        Skip to workspace
      </a>
      <header>
        <div>
          <p className="eyebrow">mslb</p>
          <h1>Simulation workspace</h1>
          <p>Configure capacity. Run repeatable experiments.</p>
        </div>
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
            <input name="token" type="password" autoComplete="off" minLength={32} maxLength={256} />
          </label>
          <div className="actions">
            <button type="submit">Connect</button>
            <button type="button" onClick={() => connect("")}>
              Clear session
            </button>
          </div>
          <small>Leave blank for local demo. Credentials stay in memory and clear on reload.</small>
        </form>
      </header>
      <QueryClientProvider client={session.client} key={session.version}>
        <Workspace api={session.api} />
      </QueryClientProvider>
      <footer>Logical simulations · Exact saved results · Local workspace</footer>
    </>
  );
}

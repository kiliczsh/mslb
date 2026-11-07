import { QueryClientProvider } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { DatasetManager } from "../features/datasets/DatasetManager.js";
import { Home } from "../features/demo/Home.js";
import { LiveDemo } from "../features/demo/LiveDemo.js";
import { DockerWorkspace } from "../features/docker/DockerWorkspace.js";
import { ServerManager } from "../features/servers/ServerManager.js";
import { RunWorkspace } from "../features/simulations/RunWorkspace.js";
import { Api } from "../shared/api/api.js";
import type { Dataset } from "../shared/api/contracts.js";
import { createQueryClient } from "../shared/queryClient.js";
import {
  CaretRightIcon,
  CompassIcon,
  CubeIcon,
  DatabaseIcon,
  FlaskIcon,
  HardDrivesIcon,
  HouseIcon,
  PlugsConnectedIcon,
  XIcon,
} from "../shared/ui/Icons.js";
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
  const [menuOpen, setMenuOpen] = useState(false);
  const header = useRef<HTMLElement>(null);
  const menuButton = useRef<HTMLButtonElement>(null);
  const connection = useRef<HTMLDetailsElement>(null);
  useEffect(() => {
    function dismiss(event: PointerEvent) {
      if (event.target instanceof Node && !header.current?.contains(event.target)) {
        setMenuOpen(false);
        if (connection.current) connection.current.open = false;
      }
    }
    function dismissOnEscape(event: KeyboardEvent) {
      if (event.key !== "Escape") return;
      if (connection.current?.open) {
        connection.current.open = false;
        connection.current.querySelector<HTMLElement>("summary")?.focus();
      } else if (menuOpen) {
        setMenuOpen(false);
        menuButton.current?.focus();
      }
    }
    document.addEventListener("pointerdown", dismiss);
    document.addEventListener("keydown", dismissOnEscape);
    return () => {
      document.removeEventListener("pointerdown", dismiss);
      document.removeEventListener("keydown", dismissOnEscape);
    };
  }, [menuOpen]);
  function navigate(next: string) {
    setTab(next);
    setMenuOpen(false);
    if (connection.current) connection.current.open = false;
    queueMicrotask(() => document.getElementById("workspace")?.focus());
  }
  const [dataset, setDataset] = useState<Dataset | null>(null);
  return (
    <WorkspaceDraftProvider>
      <a className="skip-link" href="#workspace">
        Skip to workspace
      </a>
      <header className="app-header" ref={header}>
        <button
          type="button"
          className="brand header-brand"
          aria-label="mslb home"
          onClick={() => navigate("home")}
        >
          <Ribbon compact />
          <span>mslb</span>
          <span className="brand-description">Load Balancer</span>
        </button>
        <button
          type="button"
          className="header-menu-toggle"
          ref={menuButton}
          aria-expanded={menuOpen}
          aria-controls="workspace-navigation"
          onClick={() => {
            setMenuOpen(!menuOpen);
            if (connection.current) connection.current.open = false;
          }}
        >
          Menu{" "}
          <CaretRightIcon
            size={16}
            className={`header-caret${menuOpen ? " is-open" : ""}`}
            aria-hidden="true"
          />
        </button>
        <nav
          id="workspace-navigation"
          aria-label="Workspace"
          className="workspace-nav"
          data-open={menuOpen}
        >
          {[
            { id: "home", label: "Home", Icon: HouseIcon },
            { id: "demo", label: "Demo", Icon: CompassIcon },
            { id: "servers", label: "Servers", Icon: HardDrivesIcon },
            { id: "datasets", label: "Datasets", Icon: DatabaseIcon },
            { id: "runs", label: "Simulations", Icon: FlaskIcon },
            { id: "docker", label: "Docker traffic", Icon: CubeIcon },
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
        <details
          className="session-menu"
          ref={connection}
          onToggle={(event) => {
            if (event.currentTarget.open) setMenuOpen(false);
          }}
        >
          <summary aria-label="Connection">
            <PlugsConnectedIcon size={18} aria-hidden="true" />
            <span className="connection-label">Connection</span>
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
                <PlugsConnectedIcon size={16} aria-hidden="true" /> Connect
              </button>
              <button type="button" onClick={() => connect("")}>
                <XIcon size={16} aria-hidden="true" /> Clear session
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
          <DatasetManager
            api={api}
            selected={dataset}
            onSelect={setDataset}
            onContinue={() => navigate("runs")}
          />
        )}
        {tab === "docker" && (
          <DockerWorkspace
            api={api}
            dataset={dataset}
            onChooseDataset={() => navigate("datasets")}
          />
        )}
        {tab === "runs" && (
          <RunWorkspace api={api} dataset={dataset} onChooseDataset={() => navigate("datasets")} />
        )}
      </main>
      <footer>
        <span>mslb / Simulation workspace</span>
        <span>
          {tab === "docker"
            ? "Real HTTP. Owned backend lifecycle."
            : "Logical time. Exact saved results."}
        </span>
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

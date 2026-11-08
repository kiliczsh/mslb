# mslb

Explore how a load balancer handles heterogeneous servers, queued requests and changing
capacity. mslb combines a deterministic Python simulator with a React dashboard and a
separate workspace that sends real HTTP traffic to bounded Docker backends.

## What you can do

- Add, edit and delete servers with different CPU, memory and request-start capacities.
- Compare First Fit, Round Robin, Least Connections and Most Available Memory.
- Inspect exact metrics, request timelines, playback and simulated autoscaling.
- Run real HTTP experiments in **Live Traffic**, with guarded physical scaling.
- Track durable jobs, cancel work and download verified results and matching inputs.
- Use the same application services through REST or MCP.

## Start locally

Install Docker Desktop with Linux containers, or Linux Docker Engine, and Docker Compose
2.24.4 or newer. No host Python, Node, database or Redis installation is required.

```sh
docker compose up --build -d --wait
```

The same Docker-only startup works on Windows Docker Desktop and Linux Engine; no
host-side memory script or second terminal is required. Allow at least 8 GiB of reported
Docker engine memory. The guard checks available Docker/Linux memory, container limits,
working sets and OOM observations. Missing or stale measurements pause new work; saved
results remain readable. Windows physical RAM outside Docker is not independently monitored.
If Just is installed, `just up` and `just down` wrap the Compose startup/shutdown commands.

Open the [dashboard](http://127.0.0.1:8080) or
[API reference](http://127.0.0.1:8080/api/docs). The API listens on
`http://127.0.0.1:8000`; MCP uses `http://127.0.0.1:8000/mcp`.
Migrations and the initial server configuration are automatic. Default ports are
loopback-only, with authentication disabled for the local demo.

## Try it

Choose **Start guided demo** for a small workload, playback and a four-strategy comparison.
For your own logical run:

1. Configure capacities in **Servers**.
2. Upload `docs/assignment/test-data/core/requests.csv` in **Datasets**.
3. Open **Simulations**, select the dataset and strategy, then review the frozen inputs.
4. Queue the run, inspect metrics and playback, then download `run.jsonl`.

Server changes affect future runs; saved runs retain their original configuration.
The default **sequential** mode with scaling disabled supports the unchanged supplied
validator. Select **concurrent** for the assignment's equal CPU-sharing model; an
independent replay validates that execution. Both modes are supported and documented
in [DESIGN](docs/DESIGN.md).

Browse the [24-scenario artifact index](data/test-artifacts/manifest.json) for generated
traces, complete matching bundles and validation receipts. The
[sequential example trace](data/test-artifacts/16_execution_modes/sequential/bundle/run.jsonl)
passes the unchanged supplied validator; the
[concurrent example](data/test-artifacts/05_equal_cpu_sharing/bundle/run.jsonl)
shows equal CPU sharing with independent replay.

For a physical experiment, select a dataset and open **Live Traffic**. Its observations
are actual HTTP timings and container measurements, with separate operational artifacts.

The standard installation accepts at most 64 servers, 10,000 requests and 10 MiB CSV
uploads. Physical runs are bounded to four backends, 16 in-flight calls and 120 seconds.
Resource pressure can hold admission or stop work cleanly.

```sh
docker compose down
```

Stopping retains named volumes, including datasets and saved runs.

## Documentation

Read the [documentation index](docs/README.md), [development guide](docs/DEVELOPMENT.md),
[design](docs/DESIGN.md) or [MCP guide](docs/MCP.md).
The [original assignment](docs/assignment/requirements.md) and
[AI-use summary](docs/AI-USAGE.md) are included.

The simulator, mandatory dashboard and four selected bonuses are implemented.
Observability is deferred and Grafana is excluded. Verification scope and remaining
acceptance limits are recorded in the development guide.

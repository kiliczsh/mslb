# Sparse arrivals and checkpoint boundaries

Requests arrive around ticks 49/50/51 and 99/100/101, as well as much later.

10 requests; arrival ticks 0–10000;
at most 1 arrivals in a single tick.
Workload: 8–8 units;
memory: 64–64 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| sparse-a | 8 | 256 | 4 |

## Expected behavior

- The final completion must occur at t=10001.
- This dataset covers checkpoint boundaries; crash/resume behavior requires a separate integration test.

Expected totals: 10 arrivals,
10 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

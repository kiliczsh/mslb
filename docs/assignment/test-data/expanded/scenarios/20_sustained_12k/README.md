# Sustained traffic: 12,000 requests

Ten requests per tick for 1,200 ticks exercise state accumulation during long runs.

12,000 requests; arrival ticks 0–1199;
at most 10 arrivals in a single tick.
Workload: 4–24 units;
memory: 32–256 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| fleet-00 | 64 | 4096 | 8 |
| fleet-01 | 64 | 4096 | 8 |
| fleet-02 | 64 | 4096 | 8 |
| fleet-03 | 64 | 4096 | 8 |

## Expected behavior

- Expect 12,000 unique requests and 36,000 lifecycle events.
- The active request set and pending queue must be empty at the end of the run.

Expected totals: 12000 arrivals,
12000 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Traffic phases

| Phase | Start | End (exclusive) | Requests/tick |
|---|---:|---:|---:|
| steady traffic | 0 | 1200 | 10 |

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

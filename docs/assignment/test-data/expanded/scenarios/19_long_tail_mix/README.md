# Short requests mixed with a few very long requests

Forty-one long CPU jobs and 37 high-memory jobs affect the latency distribution of short requests.

1,500 requests; arrival ticks 0–124;
at most 12 arrivals in a single tick.
Workload: 2–1200 units;
memory: 32–3072 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| fleet-00 | 64 | 4096 | 8 |
| fleet-01 | 64 | 4096 | 8 |
| fleet-02 | 64 | 4096 | 8 |
| fleet-03 | 64 | 4096 | 8 |

## Expected behavior

- Inspect p95/p99 queue and completion times as well as averages.
- Short requests must not be lost while long jobs run; every request must have a terminal event.

Expected totals: 1500 arrivals,
1500 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

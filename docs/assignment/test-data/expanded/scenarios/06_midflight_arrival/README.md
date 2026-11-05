# A new request joining an active request

A short request arriving while a long request is running joins CPU sharing from its arrival tick.

2 requests; arrival ticks 0–1;
at most 1 arrivals in a single tick.
Workload: 6–24 units;
memory: 64–64 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| join-a | 12 | 256 | 4 |

## Expected behavior

- short must finish at t=2 and long at t=3.
- The long request's completion time must not be fixed at its start.

Expected totals: 2 arrivals,
2 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

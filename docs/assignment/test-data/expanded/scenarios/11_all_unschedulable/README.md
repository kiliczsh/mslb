# Every request exceeds available capacity

The simulation must terminate even when no request can be started.

8 requests; arrival ticks 0–3;
at most 2 arrivals in a single tick.
Workload: 8–15 units;
memory: 1025–1921 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| small-a | 8 | 512 | 2 |
| small-b | 16 | 1024 | 4 |

## Expected behavior

- Expect eight arrivals, eight drops, zero starts, and zero completions.
- The final terminal event must occur at t=3.

Expected totals: 8 arrivals,
0 completions, and 8 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

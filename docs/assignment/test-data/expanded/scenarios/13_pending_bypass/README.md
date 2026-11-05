# Starting a smaller request behind a request that does not fit

Stopping the queue scan at the first request that does not fit would leave usable capacity idle.

4 requests; arrival ticks 0–2;
at most 2 arrivals in a single tick.
Workload: 4–32 units;
memory: 64–256 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| bypass-a | 8 | 256 | 4 |

## Expected behavior

- c-small and d-small must be able to start before b-large.
- b-large must eventually complete without starvation.

Expected totals: 4 arrivals,
4 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

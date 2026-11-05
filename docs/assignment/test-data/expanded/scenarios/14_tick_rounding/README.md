# Rounding execution time to tick boundaries

For a single active request, duration is ceil(work/7); completion must not be emitted on the start tick.

6 requests; arrival ticks 0–50;
at most 1 arrivals in a single tick.
Workload: 1–15 units;
memory: 64–64 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| round-a | 7 | 256 | 4 |

## Expected behavior

- Completion ticks must be 1, 11, 22, 32, 42, and 53.

Expected totals: 6 arrivals,
6 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

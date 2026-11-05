# Comparing concurrent and sequential execution of the same workload

Four equal requests run simultaneously with CPU sharing, or one at a time in sequential mode.

4 requests; arrival ticks 0–0;
at most 4 arrivals in a single tick.
Workload: 8–8 units;
memory: 128–128 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| mode-a | 8 | 1024 | 8 |

## Expected behavior

- Concurrent: four starts at t=0 and four completions at t=4.
- Sequential: starts at 0,1,2,3; completions at 1,2,3,4.

Expected totals: 4 arrivals,
4 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

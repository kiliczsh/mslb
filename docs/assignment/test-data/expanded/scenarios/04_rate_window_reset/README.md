# Resetting the admission limit each tick

With ample CPU and memory, only the limit of three starts per tick creates a queue.

12 requests; arrival ticks 0–1;
at most 7 arrivals in a single tick.
Workload: 1–1 units;
memory: 1–1 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| rate-a | 120 | 4096 | 3 |

## Expected behavior

- There must be exactly three REQUEST_STARTED events at each of t=0,1,2,3.
- All requests must finish by t=4.

Expected totals: 12 arrivals,
12 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

# Gradually increasing and decreasing traffic

Five phases show increasing and decreasing load on the same server cluster.

1,200 requests; arrival ticks 0–149;
at most 20 arrivals in a single tick.
Workload: 4–32 units;
memory: 32–256 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| fleet-00 | 64 | 4096 | 8 |
| fleet-01 | 64 | 4096 | 8 |
| fleet-02 | 64 | 4096 | 8 |
| fleet-03 | 64 | 4096 | 8 |

## Expected behavior

- Arrival counts for the five phases must be 60,240,600,240,60.
- All requests must complete with queue timeout disabled.

Expected totals: 1200 arrivals,
1200 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Traffic phases

| Phase | Start | End (exclusive) | Requests/tick |
|---|---:|---:|---:|
| warm-up | 0 | 30 | 2 |
| ramp-up | 30 | 60 | 8 |
| peak | 60 | 90 | 20 |
| ramp-down | 90 | 120 | 8 |
| normal | 120 | 150 | 2 |

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

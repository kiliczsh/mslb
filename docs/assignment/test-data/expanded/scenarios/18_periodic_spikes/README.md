# Periodic traffic spikes

A baseline of one request per tick is supplemented by 80 extra requests every 30 ticks.

660 requests; arrival ticks 0–179;
at most 81 arrivals in a single tick.
Workload: 4–16 units;
memory: 32–256 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| fleet-00 | 64 | 4096 | 8 |
| fleet-01 | 64 | 4096 | 8 |
| fleet-02 | 64 | 4096 | 8 |
| fleet-03 | 64 | 4096 | 8 |

## Expected behavior

- There must be 81 arrivals at each of ticks 0,30,60,90,120,150.
- Measure backlog and recovery between the six waves.

Expected totals: 660 arrivals,
660 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

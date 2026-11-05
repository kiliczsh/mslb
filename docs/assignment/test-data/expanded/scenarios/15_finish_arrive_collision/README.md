# Multiple completions and new arrivals on the same tick

Three different servers become free simultaneously, allowing the second wave to start on that same tick.

6 requests; arrival ticks 0–2;
at most 3 arrivals in a single tick.
Workload: 8–32 units;
memory: 128–128 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| finish-a | 8 | 128 | 2 |
| finish-b | 12 | 128 | 2 |
| finish-c | 16 | 128 | 2 |

## Expected behavior

- The first three completions must occur at t=2.
- The second wave must start at t=2 and finish at t=3.
- At t=2, completion events must be written before arrival events.

Expected totals: 6 arrivals,
6 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

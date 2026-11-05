# Traffic resuming after idle gaps

The first request arrives late; the simulation must continue across long idle intervals.

8 requests; arrival ticks 5–251;
at most 1 arrivals in a single tick.
Workload: 8–8 units;
memory: 64–64 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| idle-a | 8 | 256 | 2 |

## Expected behavior

- The first event must occur at t=5.
- The final request must finish at t=252.
- An empty queue must not cancel future arrivals.

Expected totals: 8 arrivals,
8 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

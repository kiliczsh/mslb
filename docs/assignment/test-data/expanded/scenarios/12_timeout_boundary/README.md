# Queue timeout coinciding with memory release

A request whose wait reaches the timeout is dropped before assignment, even if memory is released during the same tick.

5 requests; arrival ticks 0–7;
at most 2 arrivals in a single tick.
Workload: 8–32 units;
memory: 128–128 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| timeout-a | 8 | 128 | 1 |

## Expected behavior

- b-expire must be dropped at t=3 and c-exact at t=4, both with queue_timeout.
- d-survive must start at t=4 and finish at t=5.
- Queue timeout must not interrupt an active request.

Expected totals: 5 arrivals,
3 completions, and 2 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

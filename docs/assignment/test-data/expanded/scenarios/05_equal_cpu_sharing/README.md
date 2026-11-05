# Equal CPU sharing as the active request count changes

Twelve CPU units are shared first among three requests, then two, then one.

3 requests; arrival ticks 0–0;
at most 3 arrivals in a single tick.
Workload: 4–12 units;
memory: 64–64 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| share-a | 12 | 512 | 8 |

## Expected behavior

- Completion ticks must be 1, 2, and 3 respectively.
- Released CPU capacity must be shared among the remaining requests on the next tick.

Expected totals: 3 arrivals,
3 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.

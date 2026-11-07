import type { DockerDetail } from "../../shared/api/dockerContracts.js";
import { ScrollTable } from "./ScrollableTable.js";

const memory = (value: number | null) =>
  value === null ? "unavailable" : (value / 1024 ** 2).toFixed(3);
const seconds = (value: number | null, reset: boolean) =>
  value === null
    ? reset
      ? "unavailable (counter reset)"
      : "unavailable"
    : (value / 1_000_000).toFixed(6);
export function DockerMeasurements({ value, active }: { value: DockerDetail; active: boolean }) {
  const progress = value.progress;
  return (
    <section aria-label="Measured Docker runtime">
      <h3>{active ? "Live HTTP observations" : "Measured runtime windows"}</h3>
      {active &&
        (progress ? (
          <>
            <p>
              Observed at <time dateTime={progress.observed_at}>{progress.observed_at}</time> ·
              elapsed {progress.elapsed_seconds.toFixed(3)} seconds.
            </p>
            <p>
              Pending: {progress.pending} · controller HTTP reservations: {progress.inflight}
            </p>
            <ScrollTable label="Live backend observations">
              <table>
                <caption>
                  Fresh actual health responses; logical reservations and process memory use
                  different units.
                </caption>
                <thead>
                  <tr>
                    {[
                      "Backend",
                      "Backend inflight",
                      "Backend sample time (UTC)",
                      "Logical reservation (MB)",
                      "Process RSS (MiB)",
                      "Cgroup memory (MiB)",
                    ].map((label) => (
                      <th key={label} scope="col">
                        {label}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {progress.backends.map((sample) => (
                    <tr key={sample.resource_id}>
                      <th scope="row">{sample.health.backend_id}</th>
                      <td>{sample.health.inflight}</td>
                      <td>
                        <time dateTime={sample.observed_at}>{sample.observed_at}</time>
                      </td>
                      <td>{sample.health.reserved_mem_mb}</td>
                      <td>{memory(sample.health.rss_bytes)}</td>
                      <td>{memory(sample.health.cgroup_memory_bytes)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </ScrollTable>
            {progress.unavailable_backends.length > 0 && (
              <p>Health observation unavailable: {progress.unavailable_backends.join(", ")}.</p>
            )}
          </>
        ) : (
          <p>
            Fresh live observations are temporarily unavailable. The saved run status remains
            available.
          </p>
        ))}
      {!active &&
        (value.observations.length === 0 ? (
          <p>Runtime measurements are unavailable for this saved run.</p>
        ) : (
          <>
            <p>
              CPU/throttling are cgroup counter differences between the shown observations,
              including health overhead. Memory values are sampled peaks; activity between samples
              may be higher. Missing or reset counters stay unavailable.
            </p>
            <ScrollTable label="Measured backend windows">
              <table>
                <caption>
                  Actual per-container observation windows · at most 64 immutable identities
                </caption>
                <thead>
                  <tr>
                    {[
                      "Backend",
                      "First observation (UTC)",
                      "Last observation (UTC)",
                      "Samples",
                      "CPU seconds",
                      "Throttled seconds",
                      "Sampled peak RSS (MiB)",
                      "Sampled peak cgroup memory (MiB)",
                    ].map((label) => (
                      <th key={label} scope="col">
                        {label}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {value.observations.map((item) => (
                    <tr key={item.first.resource_id}>
                      <th scope="row">{item.first.health.backend_id}</th>
                      <td>
                        <time dateTime={item.first.observed_at}>{item.first.observed_at}</time>
                      </td>
                      <td>
                        <time dateTime={item.latest.observed_at}>{item.latest.observed_at}</time>
                      </td>
                      <td>{item.samples}</td>
                      <td>{seconds(item.cpu_delta_usec, item.cpu_counter_reset)}</td>
                      <td>{seconds(item.throttled_delta_usec, item.throttling_counter_reset)}</td>
                      <td>{memory(item.sampled_peak_rss_bytes)}</td>
                      <td>{memory(item.sampled_peak_cgroup_memory_bytes)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </ScrollTable>
          </>
        ))}
    </section>
  );
}

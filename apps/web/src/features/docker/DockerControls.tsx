import { type DockerSettings, dockerSettingsSchema } from "../../shared/api/dockerContracts.js";
import { Help } from "../../shared/ui/Help.js";
import { SlidersHorizontalIcon } from "../../shared/ui/Icons.js";

const fields = [
  ["initial_replicas", "Starting replicas", 1, 4],
  ["min_replicas", "Minimum routing backends", 1, 4],
  ["max_replicas", "Maximum routing backends", 1, 4],
  ["global_inflight", "Simultaneous requests", 1, 16],
  ["backend_inflight", "Per-backend HTTP inflight limit", 1, 4],
  ["logical_admission_mem_mb", "Logical admission reservation (MB)", 1, 1_000_000_000],
  ["starts_per_backend_second", "Starts per backend per second", 1, 32],
  ["max_pending", "Pending request limit", 1, 256],
  ["pending_timeout_seconds", "Pending timeout (seconds)", 1, 120],
  ["duration_seconds", "Duration limit (seconds)", 1, 120],
  ["request_timeout_ms", "HTTP request timeout (milliseconds)", 100, 3000],
  ["health_timeout_ms", "Health timeout (milliseconds)", 100, 1000],
  ["drain_timeout_seconds", "Drain timeout (seconds)", 1, 5],
  ["backend_memory_mb", "Dynamic container memory cap (MiB)", 96, 128],
] as const;
export function DockerControls({
  settings,
  change,
  disabled,
}: {
  settings: DockerSettings;
  change: (next: DockerSettings) => void;
  disabled: boolean;
}) {
  const advancedOptions = (
    <>
      <label>
        Dynamic container CPU cap
        <select
          value={settings.backend_nano_cpus}
          onChange={(e) => change({ ...settings, backend_nano_cpus: Number(e.target.value) })}
        >
          <option value={125_000_000}>0.125 CPU</option>
          <option value={250_000_000}>0.25 CPU</option>
          <option value={500_000_000}>0.5 CPU</option>
        </select>
      </label>
      <label>
        <input
          type="checkbox"
          checked={settings.physical_scaling.enabled}
          onChange={(e) =>
            change({
              ...settings,
              physical_scaling: { ...settings.physical_scaling, enabled: e.target.checked },
            })
          }
        />
        Enable physical autoscaling
      </label>
      {settings.physical_scaling.enabled && (
        <div className="form-grid">
          {(
            [
              ["pending_samples", "Pending samples before scale-out", 2, 10],
              ["idle_samples", "Idle samples before scale-in", 5, 30],
              ["cooldown_seconds", "Physical scaling cooldown (seconds)", 3, 30],
            ] as const
          ).map(([field, label, min, max]) => (
            <label key={field}>
              {label}
              <input
                type="number"
                min={min}
                max={max}
                step={1}
                value={
                  Number.isFinite(settings.physical_scaling[field])
                    ? settings.physical_scaling[field]
                    : ""
                }
                onChange={(e) =>
                  change({
                    ...settings,
                    physical_scaling: {
                      ...settings.physical_scaling,
                      [field]: e.target.valueAsNumber,
                    },
                  })
                }
              />
            </label>
          ))}
        </div>
      )}
      <p>
        Scaling samples use real seconds. Static backends stay running; scale-in removes only owned
        idle dynamic containers. Logical memory reserves admission capacity and does not allocate
        process RSS.
      </p>
    </>
  );
  return (
    <fieldset className="configuration-fields" disabled={disabled}>
      <legend className="sr-only">Traffic settings</legend>
      <label>
        Scheduling strategy
        <select
          value={settings.strategy}
          onChange={(e) =>
            change({
              ...settings,
              strategy: dockerSettingsSchema.shape.strategy.parse(e.target.value),
            })
          }
        >
          <option value="first_fit">First Fit</option>
          <option value="round_robin">Round Robin</option>
          <option value="least_connections">Least Connections</option>
          <option value="most_available_memory">Most Available Memory</option>
        </select>
      </label>
      {[false, true].map((advanced) => {
        const controls = (
          <div
            className={advanced ? "form-grid" : "form-grid docker-basic-fields"}
            key={advanced ? "advanced-fields" : "basic-fields"}
          >
            {fields
              .filter(
                ([field]) =>
                  advanced !==
                  ["initial_replicas", "global_inflight", "duration_seconds"].includes(field),
              )
              .map(([field, label, min, max]) => (
                <div className="control-field" key={field}>
                  <div className="field-label">
                    <label htmlFor={`docker-${field}`}>{label}</label>
                    <Help
                      text={
                        field === "logical_admission_mem_mb"
                          ? "Memory reserved for request admission; not process RAM."
                          : field === "backend_inflight"
                            ? "Maximum active HTTP requests on each backend."
                            : field === "global_inflight"
                              ? "Maximum active HTTP requests across all backends."
                              : field === "initial_replicas"
                                ? "Backends used when the run starts."
                                : field === "duration_seconds"
                                  ? "Stop accepting work after this many seconds."
                                  : `Allowed range: ${min.toLocaleString()}–${max.toLocaleString()}.`
                      }
                    />
                  </div>
                  <input
                    id={`docker-${field}`}
                    type="number"
                    min={min}
                    max={max}
                    step={1}
                    value={Number.isFinite(settings[field]) ? settings[field] : ""}
                    onChange={(e) => change({ ...settings, [field]: e.target.valueAsNumber })}
                  />
                </div>
              ))}
          </div>
        );
        return advanced ? (
          <details key="advanced">
            <summary>
              <SlidersHorizontalIcon className="summary-icon" size={16} aria-hidden="true" />{" "}
              Advanced settings
            </summary>
            {controls}
            {advancedOptions}
          </details>
        ) : (
          <div key="basic">{controls}</div>
        );
      })}
    </fieldset>
  );
}

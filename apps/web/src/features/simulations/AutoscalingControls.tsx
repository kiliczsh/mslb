import { useId } from "react";
import type { Settings } from "../../shared/api/contracts.js";

type Policy = Settings["autoscaling"];
type Props = { policy: Policy; onChange: (policy: Policy) => void };

const integers = [
  { key: "window_ticks", label: "Observation window (ticks)", min: 1, max: 10_000 },
  { key: "scale_out_queue_threshold", label: "Scale-out queue threshold", min: 0, max: 10_000 },
  { key: "cooldown_ticks", label: "Scaling cooldown (ticks)", min: 0, max: 1_000_000 },
  { key: "scale_out_step", label: "Servers added per decision", min: 1, max: 64 },
  { key: "scale_in_step", label: "Servers removed per decision", min: 1, max: 64 },
] as const;
const thresholds = [
  { key: "scale_out_cpu_threshold", label: "Scale-out CPU threshold" },
  { key: "scale_in_cpu_threshold", label: "Scale-in CPU threshold" },
  { key: "scale_out_drop_rate_threshold", label: "Scale-out drop-rate threshold" },
] as const;
const capacities = [
  { key: "cpu_units_per_tick", label: "Template CPU units per tick" },
  { key: "mem_mb", label: "Template memory capacity (MB)" },
  { key: "rate_limit_per_sec", label: "Template start limit per tick" },
] as const;

/** Directly edits the frozen wire policy; no floating-point threshold conversions. */
export function AutoscalingControls({ policy, onChange }: Props) {
  const helpId = useId();
  return (
    <details>
      <summary>Simulated autoscaling settings</summary>
      <p id={helpId}>
        This changes virtual servers. The saved policy and template remain fixed during a run.
        Disabled scaling preserves your policy for later use.
      </p>
      <label>
        Simulated autoscaling
        <select
          value={policy.enabled ? "enabled" : "disabled"}
          aria-describedby={helpId}
          onChange={(event) => onChange({ ...policy, enabled: event.target.value === "enabled" })}
        >
          <option value="disabled">Off</option>
          <option value="enabled">On</option>
        </select>
      </label>
      <div className="form-grid">
        {integers.map(({ key, label, min, max }) => (
          <label key={key}>
            {label}
            <input
              type="number"
              required
              min={min}
              max={max}
              step={1}
              value={Number.isFinite(policy[key]) ? policy[key] : ""}
              onChange={(event) => onChange({ ...policy, [key]: event.target.valueAsNumber })}
            />
          </label>
        ))}
        {(["min_servers", "max_servers"] as const).map((key) => (
          <label key={key}>
            {key === "min_servers"
              ? "Minimum servers (blank: initial count)"
              : "Maximum servers (blank: initial count + 3, capped at 64)"}
            <input
              type="number"
              min={1}
              max={64}
              step={1}
              value={policy[key] !== null && Number.isFinite(policy[key]) ? policy[key] : ""}
              onChange={(event) =>
                onChange({
                  ...policy,
                  [key]: event.target.value === "" ? null : event.target.valueAsNumber,
                })
              }
            />
          </label>
        ))}
      </div>
      <p>
        Thresholds are exact reduced fractions from 0 to 1. Scale-in CPU must be below scale-out
        CPU. Use 0/1 for zero; for example, 4/5 means 80%.
      </p>
      {thresholds.map(({ key, label }) => (
        <fieldset key={key}>
          <legend>{label}</legend>
          <div className="form-grid">
            {(["numerator", "denominator"] as const).map((part) => (
              <label key={part}>
                {label} {part}
                <input
                  type="text"
                  inputMode="numeric"
                  required
                  maxLength={part === "numerator" ? 1235 : 1234}
                  value={policy[key][part]}
                  onChange={(event) =>
                    onChange({ ...policy, [key]: { ...policy[key], [part]: event.target.value } })
                  }
                />
              </label>
            ))}
          </div>
        </fieldset>
      ))}
      <label>
        Scaling server template
        <select
          value={policy.server_template === null ? "none" : "custom"}
          onChange={(event) =>
            onChange({
              ...policy,
              server_template:
                event.target.value === "none"
                  ? null
                  : { cpu_units_per_tick: 1, mem_mb: 1, rate_limit_per_sec: 1 },
            })
          }
        >
          <option value="none">No template (scaling must be off)</option>
          <option value="custom">Explicit frozen capacities</option>
        </select>
      </label>
      {policy.server_template !== null && (
        <div className="form-grid">
          {capacities.map(({ key, label }) => (
            <label key={key}>
              {label}
              <input
                type="number"
                required
                min={1}
                max={1_000_000_000}
                step={1}
                value={
                  Number.isFinite(policy.server_template?.[key])
                    ? policy.server_template?.[key]
                    : ""
                }
                onChange={(event) => {
                  if (policy.server_template !== null)
                    onChange({
                      ...policy,
                      server_template: {
                        ...policy.server_template,
                        [key]: event.target.valueAsNumber,
                      },
                    });
                }}
              />
            </label>
          ))}
        </div>
      )}
    </details>
  );
}

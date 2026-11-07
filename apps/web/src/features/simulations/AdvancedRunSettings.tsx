import type { Settings } from "../../shared/api/contracts.js";
import { SlidersHorizontalIcon } from "../../shared/ui/Icons.js";
import { AutoscalingControls } from "./AutoscalingControls.js";

/** Edit queue, horizon, scaling and materialization budgets as one controlled form. */
export function AdvancedRunSettings({
  settings,
  onChange,
  open,
  onOpenChange,
}: {
  settings: Settings;
  onChange: (settings: Settings) => void;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  return (
    <details
      className="simulation-advanced"
      open={open}
      onToggle={(event) => onOpenChange(event.currentTarget.open)}
    >
      <summary>
        <SlidersHorizontalIcon className="summary-icon" size={16} aria-hidden="true" /> Advanced
        settings
      </summary>
      <p className="muted simulation-advanced-status">
        Queue capacity: {settings.max_queue_size ?? "no limit"} · timeout:{" "}
        {settings.queue_timeout_ticks === null ? "off" : `${settings.queue_timeout_ticks} ticks`} ·
        autoscaling: {settings.autoscaling.enabled ? "on" : "off"}
      </p>
      <div className="form-grid">
        <label>
          Queue capacity (blank: disabled)
          <input
            type="number"
            min={0}
            max={10_000}
            step={1}
            value={
              settings.max_queue_size !== null && Number.isFinite(settings.max_queue_size)
                ? settings.max_queue_size
                : ""
            }
            onChange={(event) =>
              onChange({
                ...settings,
                max_queue_size: event.target.value === "" ? null : event.target.valueAsNumber,
              })
            }
          />
        </label>
        <label>
          Queue timeout ticks (blank: disabled)
          <input
            type="number"
            min={1}
            max={1_000_000}
            step={1}
            value={
              settings.queue_timeout_ticks !== null && Number.isFinite(settings.queue_timeout_ticks)
                ? settings.queue_timeout_ticks
                : ""
            }
            onChange={(event) =>
              onChange({
                ...settings,
                queue_timeout_ticks: event.target.value === "" ? null : event.target.valueAsNumber,
              })
            }
          />
        </label>
        <label>
          Simulation horizon (ticks)
          <input
            type="number"
            required
            min={1}
            max={1_000_000}
            step={1}
            value={
              Number.isFinite(settings.max_simulation_ticks) ? settings.max_simulation_ticks : ""
            }
            onChange={(event) =>
              onChange({ ...settings, max_simulation_ticks: event.target.valueAsNumber })
            }
          />
        </label>
      </div>
      <AutoscalingControls
        policy={settings.autoscaling}
        onChange={(autoscaling) => onChange({ ...settings, autoscaling })}
      />
      <details>
        <summary>Effective execution limits</summary>
        <p>
          At most 64 logical servers, 10,000 input requests, 10 MiB CSV. Limits can be lowered;
          domain admission may reject a configuration that exceeds the installation envelope.
        </p>
        <div className="form-grid">
          {(["max_samples", "max_fraction_bits", "max_metric_series"] as const).map((key) => (
            <label key={key}>
              {
                {
                  max_samples: "Maximum samples",
                  max_fraction_bits: "Maximum exact fraction bits",
                  max_metric_series: "Maximum metric histories",
                }[key]
              }
              <input
                type="number"
                required
                min={1}
                max={
                  {
                    max_samples: 1_000_000,
                    max_fraction_bits: 4096,
                    max_metric_series: 10_000,
                  }[key]
                }
                step={1}
                value={Number.isFinite(settings[key]) ? settings[key] : ""}
                onChange={(event) => onChange({ ...settings, [key]: event.target.valueAsNumber })}
              />
            </label>
          ))}
        </div>
      </details>
    </details>
  );
}

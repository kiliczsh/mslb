import type { FormEvent } from "react";
import type { Api } from "../../shared/api/api.js";
import { type Dataset, type Settings, settingsSchema } from "../../shared/api/contracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import {
  ArrowRightIcon,
  CircleNotchIcon,
  DatabaseIcon,
  FlaskIcon,
  SlidersHorizontalIcon,
} from "../../shared/ui/Icons.js";
import { AdvancedRunSettings } from "./AdvancedRunSettings.js";

type RunFormProps = {
  dataset: Dataset | null;
  settings: Settings;
  servers: { data: Awaited<ReturnType<Api["servers"]>> | undefined; error: unknown };
  submission: {
    registering: boolean;
    reviewed: boolean;
    localError: string;
    advancedOpen: boolean;
    error: unknown;
    isError: boolean;
  };
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onChange: (settings: Settings) => void;
  onAdvancedOpenChange: (open: boolean) => void;
  onReviewReset: () => void;
  onChooseDataset: (() => void) | undefined;
};

/** Render controlled configuration and a review of the snapshot that will be submitted. */
export function RunForm({
  dataset,
  settings,
  servers,
  submission,
  onSubmit,
  onChange,
  onAdvancedOpenChange,
  onReviewReset,
  onChooseDataset,
}: RunFormProps) {
  const { registering, reviewed, localError, advancedOpen } = submission;
  return (
    <form className="run-form" noValidate onSubmit={onSubmit}>
      <fieldset className="configuration-fields" disabled={registering}>
        <legend className="sr-only">Configure a logical run</legend>
        <h3 className="simulation-section-title">
          <DatabaseIcon size={18} aria-hidden="true" /> Dataset
        </h3>
        <div className="simulation-dataset">
          <p>
            {dataset ? (
              <>
                Selected dataset: <strong>{dataset.source_filename}</strong> ·{" "}
                {dataset.request_count.toLocaleString()} requests
              </>
            ) : (
              "Select a dataset in Datasets before launching."
            )}
          </p>
          {onChooseDataset && (
            <button type="button" className="text-button" onClick={onChooseDataset}>
              {dataset ? "Change dataset" : "Choose dataset"}{" "}
              <ArrowRightIcon size={16} aria-hidden="true" />
            </button>
          )}
        </div>
        <h3 className="simulation-section-title">
          <SlidersHorizontalIcon size={18} aria-hidden="true" /> Strategy &amp; mode
        </h3>
        <div className="form-grid simulation-basics">
          <label>
            Execution mode
            <select
              aria-label="Execution mode"
              aria-describedby="simulation-mode-help"
              value={settings.mode}
              onChange={(event) =>
                onChange({
                  ...settings,
                  mode: settingsSchema.shape.mode.parse(event.target.value),
                })
              }
            >
              <option value="sequential">Sequential</option>
              <option value="concurrent">Concurrent</option>
            </select>
            <small id="simulation-mode-help" className="simulation-field-help">
              {settings.mode === "concurrent"
                ? "Active requests share each server’s CPU."
                : "One request runs at a time per server; original validator compatible."}
            </small>
          </label>
          <label>
            Scheduling strategy
            <select
              aria-label="Scheduling strategy"
              aria-describedby="simulation-strategy-help"
              value={settings.strategy}
              onChange={(event) =>
                onChange({
                  ...settings,
                  strategy: settingsSchema.shape.strategy.parse(event.target.value),
                })
              }
            >
              <option value="first_fit">First Fit</option>
              <option value="round_robin">Round Robin</option>
              <option value="least_connections">Least Connections</option>
              <option value="most_available_memory">Most Available Memory</option>
            </select>
            <small id="simulation-strategy-help" className="simulation-field-help">
              {
                {
                  first_fit: "Uses the first eligible server by ID.",
                  round_robin: "Rotates through eligible servers.",
                  least_connections: "Chooses the eligible server with the fewest active requests.",
                  most_available_memory: "Chooses the eligible server with the most free memory.",
                }[settings.strategy]
              }
            </small>
          </label>
        </div>
        <AdvancedRunSettings
          settings={settings}
          onChange={onChange}
          open={advancedOpen}
          onOpenChange={onAdvancedOpenChange}
        />
        <p className="muted simulation-effective">
          Queue:{" "}
          {settings.max_queue_size === null
            ? "no capacity limit"
            : `${settings.max_queue_size} requests`}{" "}
          · timeout:{" "}
          {settings.queue_timeout_ticks === null ? "off" : `${settings.queue_timeout_ticks} ticks`}{" "}
          · autoscaling: {settings.autoscaling.enabled ? "on" : "off"}
        </p>
      </fieldset>
      <Feedback error={servers.error} />
      <Feedback error={submission.error} />
      {localError && <p role="alert">{localError}</p>}
      {submission.isError && (
        <p className="notice">
          A retry with unchanged settings reuses the same idempotency key. Check history if the
          previous response was interrupted.
        </p>
      )}
      {reviewed && (
        <div className="notice" role="status">
          <h3 className="simulation-section-title">Review snapshot inputs</h3>
          <p>
            {dataset?.source_filename} · {dataset?.request_count.toLocaleString()} requests ·{" "}
            {settings.strategy.replaceAll("_", " ")} · {settings.mode}.
          </p>
          <p>
            Queue capacity: {settings.max_queue_size ?? "no limit"} · timeout:{" "}
            {settings.queue_timeout_ticks ?? "off"}
            {settings.queue_timeout_ticks !== null ? " ticks" : ""} · autoscaling:{" "}
            {settings.autoscaling.enabled ? "on" : "off"}.
          </p>
          <details className="simulation-review-details">
            <summary>Server capacities and versions</summary>
            <ul>
              {servers.data?.items.map((server) => (
                <li key={server.id}>
                  {server.id} · CPU {server.cpu_units_per_tick} · memory {server.mem_mb} MB · starts{" "}
                  {server.rate_limit_per_sec}/tick · version {server.version}
                </li>
              ))}
            </ul>
          </details>
          <details className="simulation-review-details">
            <summary>Configuration to submit</summary>
            <pre>{JSON.stringify(settings, null, 2)}</pre>
          </details>
        </div>
      )}
      <button
        type="submit"
        className="primary"
        disabled={!dataset || !servers.data?.items.length || registering}
      >
        {registering ? (
          <CircleNotchIcon className="icon-spin" size={18} aria-hidden="true" />
        ) : (
          <FlaskIcon size={18} aria-hidden="true" />
        )}
        {registering ? "Registering run…" : reviewed ? "Confirm and queue run" : "Review snapshot"}
        <ArrowRightIcon size={18} aria-hidden="true" />
      </button>
      {reviewed && !registering && (
        <button type="button" className="text-button" onClick={() => onReviewReset()}>
          Back to configuration
        </button>
      )}
    </form>
  );
}

import type { Api } from "../../shared/api/api.js";
import { type Settings, terminal } from "../../shared/api/contracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import { CopyIcon } from "../../shared/ui/Icons.js";
import { StatusIcon } from "../../shared/ui/StatusIcon.js";
import { failureMessage } from "../history/historyContracts.js";

/** Explain durable status, failures and the exact saved inputs for a selected run. */
export function RunSummary({
  run,
  cancelling,
  cancelError,
  onCancel,
  onCopy,
}: {
  run: Awaited<ReturnType<Api["run"]>>;
  cancelling: boolean;
  cancelError: unknown;
  onCancel: () => void;
  onCopy: (settings: Settings) => void;
}) {
  return (
    <div className="panel result-summary">
      <p className="mono">{run.id}</p>
      <p className="status icon-label">
        <StatusIcon status={run.status} />
        {run.status.replaceAll("_", " ")}
      </p>
      <p>
        {run.mode} · {run.strategy} · attempt {run.attempts}
      </p>
      {run.progress && (
        <p>
          Committed through tick {run.progress.next_tick}; {run.progress.committed_events} events
          saved.
        </p>
      )}
      {run.error_code && (
        <div className="notice error" role="alert">
          <strong>{failureMessage(run.error_code)}</strong>
          {run.error_code === "STORAGE_LIMIT_EXCEEDED" && (
            <p>
              Large pending queues can create large playback files. This failed run has no completed
              result bundle.
            </p>
          )}
          <small>Diagnostic: {run.error_code}</small>
        </div>
      )}
      {!terminal(run.status) && (
        <button
          type="button"
          disabled={cancelling || run.status === "cancel_requested"}
          onClick={() => onCancel()}
        >
          {run.status === "cancel_requested" ? "Cancellation pending" : "Cancel simulation"}
        </button>
      )}
      <Feedback error={cancelError} />
      <details>
        <summary>Validation and saved configuration</summary>
        {run.validation && (
          <>
            <p>
              Independent replay: {run.validation.independent_replay}. Original validator:{" "}
              {run.validation.original_validator.replaceAll("_", " ")}.
            </p>
            <p>
              Terminal tick {run.validation.terminal_tick} · {run.validation.event_count} events
            </p>
          </>
        )}
        <p className="mono">Snapshot: {run.snapshot_digest}</p>
        <p className="mono">Dataset: {run.dataset_digest}</p>
        <p className="mono">Server snapshot: {run.server_snapshot_digest}</p>
        <pre>{JSON.stringify(run.configuration, null, 2)}</pre>
        <button
          type="button"
          onClick={() => {
            onCopy(structuredClone(run.configuration));
          }}
        >
          <CopyIcon size={16} aria-hidden="true" /> Copy settings for a new run
        </button>
      </details>
    </div>
  );
}

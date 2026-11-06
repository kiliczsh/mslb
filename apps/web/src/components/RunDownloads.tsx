import { useEffect, useRef, useState } from "react";
import type { Api } from "../lib/api.js";
import {
  type Artifact,
  artifactTypes,
  browserDownload,
  isArtifact,
} from "../lib/browserDownload.js";
import { Feedback } from "./Feedback.js";
import { DownloadSimpleIcon } from "./Icons.js";

export function RunDownloads({ api, runId }: { api: Api; runId: string }) {
  const [selected, setSelected] = useState<Artifact>("run.jsonl");
  const [busy, setBusy] = useState(false),
    [bytes, setBytes] = useState(0),
    [error, setError] = useState<unknown>(null),
    [finished, setFinished] = useState(false);
  const active = useRef<AbortController | null>(null);
  useEffect(() => () => active.current?.abort(), []);
  async function download() {
    if (active.current) return;
    const controller = new AbortController();
    active.current = controller;
    setBusy(true);
    setFinished(false);
    setBytes(0);
    setError(null);
    let lastUpdate = 0;
    try {
      const path =
        selected === "inputs.zip" ? "inputs" : `artifact?name=${encodeURIComponent(selected)}`;
      await api.response(
        `simulations/${encodeURIComponent(runId)}/${path}`,
        (response, signal) =>
          browserDownload(response, signal, selected, (count) => {
            if (Date.now() - lastUpdate > 100) {
              lastUpdate = Date.now();
              setBytes(count);
            }
          }),
        { signal: controller.signal },
        300_000,
      );
      setFinished(true);
    } catch (failure) {
      if (!controller.signal.aborted) setError(failure);
    } finally {
      active.current = null;
      setBusy(false);
    }
  }
  return (
    <section aria-label="Saved artifact downloads">
      <h3>Download verified results</h3>
      <label>
        Saved artifact
        <select
          value={selected}
          disabled={busy}
          onChange={(event) => {
            const choice = event.target.value;
            if (isArtifact(choice)) setSelected(choice);
          }}
        >
          {Object.keys(artifactTypes).map((name) => (
            <option key={name} value={name}>
              {name}
            </option>
          ))}
        </select>
      </label>
      <div className="actions">
        <button
          type="button"
          disabled={busy}
          onClick={() => {
            void download();
          }}
        >
          <DownloadSimpleIcon size={18} aria-hidden="true" />
          Download saved artifact
        </button>
        {busy && (
          <button type="button" onClick={() => active.current?.abort()}>
            Cancel download
          </button>
        )}
      </div>
      {busy && (
        <p role="status">Streaming to browser · {bytes.toLocaleString()} bytes transferred</p>
      )}
      {finished && <p role="status">File handed to browser. Check the browser's download list.</p>}
      <Feedback error={error} />
      <p>
        Files stream to the browser without a whole-file preview. The input ZIP contains normalized
        reproducibility files. Authentication stays in request headers.
      </p>
    </section>
  );
}

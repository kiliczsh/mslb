import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import type { Api } from "../../shared/api/api.js";
import type { Dataset } from "../../shared/api/contracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";

export function DatasetManager({
  api,
  selected,
  onSelect,
}: {
  api: Api;
  selected: Dataset | null;
  onSelect: (dataset: Dataset) => void;
}) {
  const [cursor, setCursor] = useState<string | null>(null);
  const [file, setFile] = useState<File | null>(null);
  const client = useQueryClient();
  const datasets = useQuery({
    queryKey: ["datasets", cursor],
    queryFn: ({ signal }) => api.datasets(cursor, signal),
  });
  const upload = useMutation({
    mutationFn: (source: File) => api.upload(source),
    onSuccess: async (dataset) => {
      onSelect(dataset);
      setCursor(null);
      await client.invalidateQueries({ queryKey: ["datasets"] });
    },
  });
  return (
    <section aria-labelledby="datasets-heading">
      <h2 id="datasets-heading">Request datasets</h2>
      <p>Upload an immutable CSV with columns t, request_id, work_units, mem_mb.</p>
      <form
        className="panel"
        onSubmit={(event) => {
          event.preventDefault();
          if (file) upload.mutate(file);
        }}
      >
        <label>
          Requests CSV
          <input
            type="file"
            accept=".csv,text/csv"
            required
            onChange={(event) => {
              setFile(event.target.files?.item(0) ?? null);
              upload.reset();
            }}
          />
        </label>
        <p className="muted">Maximum 10 MiB and 10,000 requests. CSV row errors appear below.</p>
        <button type="submit" className="primary" disabled={!file || upload.isPending}>
          {upload.isPending ? "Validating upload…" : "Upload dataset"}
        </button>
        <Feedback error={upload.error} />
        {upload.isSuccess && <p role="status">Dataset registered and selected.</p>}
      </form>
      <Feedback error={datasets.error} />
      {datasets.isPending && <p role="status">Loading datasets…</p>}
      {datasets.data?.items.length === 0 && <p>No datasets yet. Upload a CSV to begin.</p>}
      <div className="dataset-list">
        {datasets.data?.items.map((dataset) => (
          <article className="panel" key={dataset.id}>
            <div className="section-heading">
              <h3>{dataset.source_filename}</h3>
              <button
                type="button"
                aria-pressed={selected?.id === dataset.id}
                onClick={() => onSelect(dataset)}
              >
                {selected?.id === dataset.id
                  ? "Selected dataset"
                  : `Select ${dataset.source_filename}`}
              </button>
            </div>
            <p>
              {dataset.request_count.toLocaleString()} requests · arrivals{" "}
              {dataset.earliest_arrival}–{dataset.latest_arrival} ·{" "}
              {dataset.size_bytes.toLocaleString()} normalized bytes
            </p>
            <p className="mono">ID {dataset.id}</p>
            <details>
              <summary>Integrity digests</summary>
              <p className="mono">Normalized SHA-256: {dataset.digest}</p>
              <p className="mono">Original SHA-256: {dataset.original_digest ?? "Not recorded"}</p>
            </details>
          </article>
        ))}
      </div>
      <div className="actions">
        <button type="button" disabled={cursor === null} onClick={() => setCursor(null)}>
          First datasets
        </button>
        <button
          type="button"
          disabled={!datasets.data?.next_cursor}
          onClick={() => setCursor(datasets.data?.next_cursor ?? null)}
        >
          Next datasets
        </button>
      </div>
    </section>
  );
}

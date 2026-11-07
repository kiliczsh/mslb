import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useDraftState, useDraftStorage } from "../../app/workspaceDrafts.js";
import type { Api } from "../../shared/api/api.js";
import type { Dataset } from "../../shared/api/contracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import {
  ArrowRightIcon,
  CheckCircleIcon,
  CircleNotchIcon,
  EyeIcon,
  FileCsvIcon,
  MagnifyingGlassIcon,
  UploadSimpleIcon,
  XIcon,
} from "../../shared/ui/Icons.js";

export function DatasetManager({
  api,
  selected,
  onSelect,
  onContinue,
}: {
  api: Api;
  selected: Dataset | null;
  onSelect: (dataset: Dataset | null) => void;
  onContinue?: () => void;
}) {
  const storage = useDraftStorage();
  const [, setSelectedRun] = useDraftState({ storage, name: "selectedRun" });
  const [cursor, setCursor] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [file, setFile] = useState<File | null>(null);
  const client = useQueryClient();
  const datasets = useQuery({
    queryKey: ["datasets", cursor],
    queryFn: ({ signal }) => api.datasets(cursor, signal),
  });
  const preview = useQuery({
    queryKey: ["dataset-preview", selected?.id],
    queryFn: ({ signal }) => {
      if (!selected) throw new Error("Select a dataset first.");
      return api.datasetPreview({ id: selected.id, signal });
    },
    enabled: selected !== null,
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
    <section aria-label="Datasets" className="datasets-workspace">
      <p className="eyebrow dataset-workspace-label">Datasets</p>
      <form
        className="dataset-upload"
        onSubmit={(event) => {
          event.preventDefault();
          if (file) upload.mutate(file);
        }}
      >
        <label className="dataset-file">
          Requests CSV
          <input
            type="file"
            accept=".csv,text/csv"
            required
            disabled={upload.isPending}
            aria-describedby="dataset-upload-help"
            onChange={(event) => {
              setFile(event.target.files?.item(0) ?? null);
              upload.reset();
            }}
          />
        </label>
        <button type="submit" className="primary" disabled={!file || upload.isPending}>
          {upload.isPending ? (
            <CircleNotchIcon className="icon-spin" size={16} aria-hidden="true" />
          ) : (
            <UploadSimpleIcon size={16} aria-hidden="true" />
          )}
          {upload.isPending ? "Validating upload…" : "Upload dataset"}
        </button>
        <p id="dataset-upload-help" className="dataset-upload-help muted">
          CSV columns: <code>t, request_id, work_units, mem_mb</code> · Up to 10 MiB / 10,000
          requests.
        </p>
        <Feedback error={upload.error} />
        {upload.isSuccess && (
          <p className="dataset-upload-feedback" role="status">
            Dataset registered and selected.
          </p>
        )}
      </form>
      <Feedback error={datasets.error} />
      {datasets.isPending && <p role="status">Loading datasets…</p>}
      {datasets.data?.items.length === 0 && (
        <p className="dataset-meta">No datasets yet. Upload a CSV to begin.</p>
      )}
      {selected && (
        <section className="dataset-preview" aria-label="Selected CSV preview">
          <div className="selection-bar dataset-selection">
            <span>
              Selected: <strong>{selected.source_filename}</strong>
            </span>
            <div className="dataset-selection-actions">
              <button type="button" onClick={() => onSelect(null)}>
                <XIcon size={16} aria-hidden="true" /> Unselect
              </button>
              {onContinue && (
                <button
                  type="button"
                  className="primary"
                  onClick={() => {
                    setSelectedRun(null);
                    onContinue();
                  }}
                >
                  Continue to simulation <ArrowRightIcon size={16} aria-hidden="true" />
                </button>
              )}
            </div>
          </div>
          {preview.isPending && (
            <p role="status" className="icon-label">
              <CircleNotchIcon className="icon-spin" size={16} aria-hidden="true" /> Loading CSV
              preview…
            </p>
          )}
          <Feedback error={preview.error} />
          {preview.isError && (
            <button type="button" onClick={() => void preview.refetch()}>
              Retry preview
            </button>
          )}
          {preview.data && (
            <section className="table-scroll dataset-preview-table" aria-label="CSV rows">
              <table>
                <caption>
                  Normalized CSV · first {preview.data.rows.length} of{" "}
                  {preview.data.total_rows.toLocaleString()} rows
                </caption>
                <thead>
                  <tr>
                    <th scope="col">t</th>
                    <th scope="col">request_id</th>
                    <th scope="col">work_units</th>
                    <th scope="col">mem_mb</th>
                  </tr>
                </thead>
                <tbody>
                  {preview.data.rows.map((row) => (
                    <tr key={row.request_id}>
                      <td>{row.t}</td>
                      <th scope="row">{row.request_id}</th>
                      <td>{row.work_units}</td>
                      <td>{row.mem_mb}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>
          )}
        </section>
      )}
      <div className="dataset-list-toolbar">
        <p className="dataset-page-count muted">
          {datasets.data ? `${datasets.data.items.length} datasets on this page` : "Saved datasets"}
        </p>
        <label className="dataset-search">
          <MagnifyingGlassIcon size={16} aria-hidden="true" />
          <span className="sr-only">Find a dataset on this page</span>
          <input
            type="search"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
            placeholder="Search filename or ID"
          />
        </label>
      </div>
      {datasets.data &&
        !datasets.data.items.some((dataset) =>
          `${dataset.source_filename} ${dataset.id}`.toLowerCase().includes(search.toLowerCase()),
        ) &&
        search && <p role="status">No matching datasets on this page.</p>}
      <div className="dataset-list">
        {datasets.data?.items
          .filter((dataset) =>
            `${dataset.source_filename} ${dataset.id}`.toLowerCase().includes(search.toLowerCase()),
          )
          .map((dataset) => (
            <article
              className="dataset-row"
              data-selected={selected?.id === dataset.id}
              key={dataset.id}
            >
              <div className="dataset-row-info">
                <h3 className="dataset-title">{dataset.source_filename}</h3>
                <p className="dataset-meta muted">
                  {dataset.request_count.toLocaleString()} requests · arrivals{" "}
                  {dataset.earliest_arrival}–{dataset.latest_arrival} ·{" "}
                  {dataset.size_bytes.toLocaleString()} bytes (normalized)
                </p>
              </div>
              <button
                type="button"
                aria-pressed={selected?.id === dataset.id}
                aria-label={`${selected?.id === dataset.id ? "Unselect" : "Select"} ${dataset.source_filename}`}
                onClick={() => onSelect(selected?.id === dataset.id ? null : dataset)}
              >
                {selected?.id === dataset.id ? (
                  <CheckCircleIcon size={16} aria-hidden="true" />
                ) : (
                  <FileCsvIcon size={16} aria-hidden="true" />
                )}
                {selected?.id === dataset.id ? "Unselect" : "Select"}
              </button>

              <details className="dataset-details">
                <summary>
                  <EyeIcon className="summary-icon" size={16} aria-hidden="true" /> Details
                </summary>
                <p className="mono">ID {dataset.id}</p>
                <p className="mono">Normalized SHA-256: {dataset.digest}</p>
                <p className="mono">
                  Original SHA-256: {dataset.original_digest ?? "Not recorded"}
                </p>
              </details>
            </article>
          ))}
      </div>
      {(cursor !== null || datasets.data?.next_cursor) && (
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
      )}
    </section>
  );
}

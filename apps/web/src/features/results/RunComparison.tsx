import { useMutation, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { type Api, ApiError } from "../../shared/api/api.js";
import type { Run } from "../../shared/api/contracts.js";
import { Feedback } from "../../shared/ui/Feedback.js";
import { MetricNumber } from "../../shared/ui/MetricNumber.js";
import { compatibleRuns } from "./comparison.js";

export function RunComparison({ api, run }: { api: Api; run: Run }) {
  const [cursor, setCursor] = useState<string | null>(null);
  const [chosen, setChosen] = useState<Run[]>([run]);
  const [baseline, setBaseline] = useState(run.id);
  const [submitted, setSubmitted] = useState<string[] | null>(null);
  const [metric, setMetric] = useState("completed_throughput");
  const [lookupId, setLookupId] = useState("");
  const lookup = useMutation({
    mutationFn: async (id: string) => {
      const candidate = await api.run(id, new AbortController().signal);
      if (!compatibleRuns(run, candidate))
        throw new ApiError(
          "This run is incomplete or has incompatible frozen inputs/settings.",
          "CONFLICT",
        );
      return candidate;
    },
    onSuccess: (candidate) => {
      setChosen((previous) =>
        previous.some((value) => value.id === candidate.id)
          ? previous
          : [...previous, candidate].slice(0, 8),
      );
      setBaseline((previous) => previous || candidate.id);
      setSubmitted(null);
    },
  });
  const history = useQuery({
    queryKey: ["runs", cursor],
    queryFn: ({ signal }) => api.runs(cursor, signal),
  });
  const result = useQuery({
    queryKey: ["comparison", submitted],
    enabled: submitted !== null,
    queryFn: ({ signal }) => api.compare(submitted ?? [], signal),
  });
  return (
    <section aria-label="Run comparison">
      <h3>Compare compatible completed runs</h3>
      <p>
        Choose 2–8 runs. Dataset, initial servers, mode, engine and other semantic settings must
        match. Strategy and simulated autoscaling may differ. The backend validates compatibility
        again.
      </p>
      <Feedback error={history.error} />
      <form
        onSubmit={(event) => {
          event.preventDefault();
          lookup.mutate(lookupId);
        }}
      >
        <label>
          Saved comparison run ID
          <input
            required
            maxLength={36}
            pattern="[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}"
            value={lookupId}
            onChange={(event) => setLookupId(event.target.value)}
          />
        </label>
        <button type="submit" disabled={chosen.length >= 8 || lookup.isPending}>
          Add saved run
        </button>
        <Feedback error={lookup.error} />
      </form>
      <ul className="comparison-picker">
        {history.data?.items.map((candidate) => (
          <li key={candidate.id}>
            <label>
              <input
                type="checkbox"
                checked={chosen.some((value) => value.id === candidate.id)}
                disabled={
                  !compatibleRuns(run, candidate) ||
                  (chosen.length >= 8 && !chosen.some((value) => value.id === candidate.id))
                }
                onChange={(event) => {
                  setSubmitted(null);
                  if (event.target.checked) setChosen([...chosen, candidate]);
                  else {
                    const remaining = chosen.filter((value) => value.id !== candidate.id);
                    setChosen(remaining);
                    if (candidate.id === baseline) setBaseline(remaining[0]?.id ?? "");
                  }
                }}
              />
              {candidate.id} · {candidate.configuration.strategy} · {candidate.status}
              {compatibleRuns(run, candidate) ? "" : " · incompatible or incomplete"}
            </label>
          </li>
        ))}
      </ul>
      <div className="actions">
        <button type="button" disabled={cursor === null} onClick={() => setCursor(null)}>
          First comparison runs
        </button>
        <button
          type="button"
          disabled={!history.data?.next_cursor}
          onClick={() => setCursor(history.data?.next_cursor ?? null)}
        >
          Next comparison runs
        </button>
      </div>
      <label>
        Explicit baseline
        <select
          value={baseline}
          onChange={(event) => {
            setBaseline(event.target.value);
            setSubmitted(null);
          }}
        >
          {chosen.length === 0 && <option value="">Choose a run</option>}
          {chosen.map((value) => (
            <option key={value.id} value={value.id}>
              {value.id} · {value.configuration.strategy}
            </option>
          ))}
        </select>
      </label>
      <p>{chosen.length} selected across history pages. No strategy is a universal winner.</p>
      <button
        type="button"
        disabled={chosen.length < 2 || !baseline || result.isFetching}
        onClick={() =>
          setSubmitted([
            baseline,
            ...chosen.filter((value) => value.id !== baseline).map((value) => value.id),
          ])
        }
      >
        Compare selected runs
      </button>
      <Feedback error={result.error} />
      {result.isFetching && <p role="status">Reading exact baseline differences…</p>}
      {result.data && (
        <>
          <p className="mono">Baseline: {result.data.baseline_run_id}</p>
          <label>
            Comparison metric
            <select value={metric} onChange={(event) => setMetric(event.target.value)}>
              {Object.keys(result.data.runs[0]?.metrics ?? {}).map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
          </label>
          <div className="table-scroll">
            <table>
              <caption>Exact baseline comparison · {metric}</caption>
              <thead>
                <tr>
                  <th scope="col">Run / strategy</th>
                  <th scope="col">Baseline value</th>
                  <th scope="col">Run value</th>
                  <th scope="col">Difference</th>
                  <th scope="col">Difference (%)</th>
                </tr>
              </thead>
              <tbody>
                {result.data.runs.map((value) => (
                  <tr key={value.run_id}>
                    <th scope="row">
                      {value.run_id}
                      <br />
                      {value.configuration.strategy}
                    </th>
                    {(
                      ["baseline", "candidate", "difference", "percentage_difference"] as const
                    ).map((field) => (
                      <td key={field}>
                        <MetricNumber value={value.metrics[metric]?.[field] ?? null} />
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p>
            Percentage difference is undefined when the baseline is zero. Undefined latency remains
            undefined.
          </p>
          {result.data.runs.map((value) => (
            <details key={value.run_id}>
              <summary>Saved settings and all exact differences · {value.run_id}</summary>
              <p>Differing settings: {value.differing_settings.join(", ") || "none"}</p>
              <pre>{JSON.stringify(value, null, 2)}</pre>
            </details>
          ))}
        </>
      )}
    </section>
  );
}

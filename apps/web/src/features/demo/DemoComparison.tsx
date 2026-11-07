import { useEffect, useRef, useState } from "react";
import type { z } from "zod";
import type { comparisonSchema } from "../../shared/api/resultContracts.js";
import { MetricNumber } from "../results/MetricNumber.js";
import { strategyLabels } from "./demoState.js";

export function DemoComparison({
  comparison,
  onOpenRun,
}: {
  comparison: z.infer<typeof comparisonSchema>;
  onOpenRun: (id: string) => void;
}) {
  const viewport = useRef<HTMLDivElement>(null);
  const [position, setPosition] = useState({ left: 0, maximum: 0 });
  useEffect(() => {
    if (!comparison.runs.length) return;
    const element = viewport.current;
    if (!element) return;
    const update = () =>
      setPosition({
        left: element.scrollLeft,
        maximum: Math.max(0, element.scrollWidth - element.clientWidth),
      });
    const observer = new ResizeObserver(update);
    observer.observe(element);
    update();
    return () => observer.disconnect();
  }, [comparison]);
  return (
    <section aria-label="Guided strategy comparison">
      {position.maximum > 0 && (
        <div className="actions">
          <button
            type="button"
            aria-controls="guided-comparison-scroll"
            disabled={position.left <= 0}
            onClick={() => viewport.current?.scrollBy({ left: -240, behavior: "auto" })}
          >
            Scroll comparison left
          </button>
          <button
            type="button"
            aria-controls="guided-comparison-scroll"
            disabled={position.left >= position.maximum - 1}
            onClick={() => viewport.current?.scrollBy({ left: 240, behavior: "auto" })}
          >
            Scroll comparison right
          </button>
        </div>
      )}
      <div
        className="table-scroll"
        id="guided-comparison-scroll"
        ref={viewport}
        onScroll={(event) => {
          const element = event.currentTarget;
          setPosition({
            left: element.scrollLeft,
            maximum: Math.max(0, element.scrollWidth - element.clientWidth),
          });
        }}
      >
        <table>
          <caption>
            Four saved runs · throughput in requests/tick · response in ticks · First Fit baseline
          </caption>
          <thead>
            <tr>
              <th scope="col">Strategy</th>
              <th scope="col">Throughput</th>
              <th scope="col">Avg response</th>
              <th scope="col">Response difference</th>
              <th scope="col">Saved run</th>
            </tr>
          </thead>
          <tbody>
            {comparison.runs.map((run) => (
              <tr key={run.run_id}>
                <th scope="row">{strategyLabels[run.configuration.strategy]}</th>
                <td>
                  <MetricNumber value={run.metrics.completed_throughput?.candidate ?? null} />
                </td>
                <td>
                  <MetricNumber value={run.metrics["response.mean"]?.candidate ?? null} />
                </td>
                <td>
                  <MetricNumber value={run.metrics["response.mean"]?.difference ?? null} />
                </td>
                <td>
                  <button type="button" onClick={() => onOpenRun(run.run_id)}>
                    Inspect {strategyLabels[run.configuration.strategy]}
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

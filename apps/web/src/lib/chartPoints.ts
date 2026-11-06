import { chartNumber } from "./metricValues.js";
import type { Chart } from "./resultContracts.js";

type PlotPoint = { t: number; minimum: number | null; maximum: number | null; mean: number | null };

/** Close covered buckets and explicitly break missing ranges between them. */
export function chartPoints(points: Chart["points"]): PlotPoint[] {
  const plotted: PlotPoint[] = [];
  let last: PlotPoint | null = null;
  let end = 0;
  for (const point of points) {
    if (last && point.t > end) {
      plotted.push({ ...last, t: end });
      plotted.push({ t: end, minimum: null, maximum: null, mean: null });
    }
    last = {
      t: point.t,
      minimum: chartNumber(point.min),
      maximum: chartNumber(point.max),
      mean: chartNumber(point.mean),
    };
    plotted.push(last);
    end = point.end_tick;
  }
  if (last) plotted.push({ ...last, t: end });
  return plotted;
}

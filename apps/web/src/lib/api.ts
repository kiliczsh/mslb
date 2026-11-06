import type { z } from "zod";
import {
  acceptedSchema,
  datasetSchema,
  detailSchema,
  errorSchema,
  pageSchema,
  readinessSchema,
  runSchema,
  type ServerInput,
  type Settings,
  serverSchema,
} from "./contracts.js";
import {
  type Chart,
  chartSchema,
  comparisonSchema,
  eventSchema,
  metricsSchema,
  type PlaybackSelection,
  playbackSchema,
  serverMetricSchema,
} from "./resultContracts.js";

export class ApiError extends Error {
  constructor(
    message: string,
    readonly code = "UNAVAILABLE",
    readonly requestId = "",
    readonly fields: readonly { field: string; reason: string }[] = [],
  ) {
    super(message);
  }
}

export async function boundedJson(response: Response): Promise<unknown> {
  const reader = response.body?.getReader();
  if (!reader) throw new ApiError("The service returned an empty response.", "INVALID_RESPONSE");
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > 1024 ** 2) throw new ApiError("Response exceeds 1 MiB.", "INVALID_RESPONSE");
      chunks.push(value);
    }
  } finally {
    await reader.cancel();
    reader.releaseLock();
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  try {
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  } catch {
    throw new ApiError("The service returned invalid JSON.", "INVALID_RESPONSE");
  }
}

/** One same-origin transport; the bearer exists only in this in-memory session. */
export class Api {
  private readonly lifetime = new AbortController();
  private pending = 0;
  constructor(
    private token = "",
    private readonly fetcher: typeof fetch = globalThis.fetch.bind(globalThis),
  ) {}

  close() {
    this.token = "";
    this.lifetime.abort();
  }

  async request<T>(path: string, schema: z.ZodType<T>, init: RequestInit = {}): Promise<T> {
    if (this.pending >= 8)
      throw new ApiError(
        "This browser session is busy. Wait for an active request, then retry.",
        "RATE_LIMITED",
      );
    const headers = new Headers(init.headers);
    if (this.token) headers.set("Authorization", `Bearer ${this.token}`);
    this.pending += 1;
    try {
      const response = await this.fetcher(`/api/v1/${path}`, {
        ...init,
        headers,
        credentials: "omit",
        cache: "no-store",
        redirect: "error",
        signal: AbortSignal.any([
          this.lifetime.signal,
          AbortSignal.timeout(30_000),
          ...(init.signal ? [init.signal] : []),
        ]),
      });
      if (response.status === 204) return schema.parse(undefined);
      const value = await boundedJson(response);
      // Product readiness uses a 503 success-shaped diagnostic while the product is incomplete.
      if (!response.ok && !(path === "health/ready" && readinessSchema.safeParse(value).success)) {
        const parsed = errorSchema.safeParse(value);
        if (parsed.success)
          throw new ApiError(
            parsed.data.error.message,
            parsed.data.error.code,
            parsed.data.request_id,
            parsed.data.error.details,
          );
        throw new ApiError(`The service could not complete this request (${response.status}).`);
      }
      const parsed = schema.safeParse(value);
      if (!parsed.success)
        throw new ApiError("The service response does not match its contract.", "INVALID_RESPONSE");
      return parsed.data;
    } catch (error) {
      if (error instanceof ApiError || init.signal?.aborted) throw error;
      // Browser/library exceptions may contain request details. Do not echo them into the UI/logs.
      throw new ApiError("Cannot reach the service. Check connectivity, then retry.");
    } finally {
      this.pending -= 1;
    }
  }

  servers = (signal: AbortSignal) =>
    this.request("servers?limit=64", pageSchema(serverSchema), { signal });
  datasets = (cursor: string | null, signal: AbortSignal) =>
    this.request(
      `datasets?limit=20${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`,
      pageSchema(datasetSchema),
      { signal },
    );
  runs = (cursor: string | null, signal: AbortSignal) =>
    this.request(
      `simulations?limit=20${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`,
      pageSchema(runSchema),
      { signal },
    );
  run = (id: string, signal: AbortSignal) =>
    this.request(`simulations/${encodeURIComponent(id)}`, detailSchema, { signal });
  ready = (signal: AbortSignal) => this.request("health/ready", readinessSchema, { signal });
  createServer = (body: ServerInput) => this.request("servers", serverSchema, json(body));
  updateServer = ({ id, ...body }: ServerInput, version: number) =>
    this.request(`servers/${encodeURIComponent(id)}`, serverSchema, {
      ...json(body, "PATCH"),
      headers: {
        "Content-Type": "application/json",
        "If-Match": `"${version}"`,
      },
    });
  async upload(file: File) {
    if (file.size > 10 * 1024 ** 2 || file.size === 0)
      throw new ApiError("Choose a nonempty CSV of at most 10 MiB.", "VALIDATION_ERROR");
    const body = new FormData();
    body.append("file", file);
    return this.request("datasets", datasetSchema, { method: "POST", body });
  }
  submit = (datasetId: string, settings: Settings, key: string) =>
    this.request("simulations", acceptedSchema, {
      ...json({ ...settings, dataset_id: datasetId }),
      headers: { "Content-Type": "application/json", "Idempotency-Key": key },
    });
  cancel = (id: string) =>
    this.request(`simulations/${encodeURIComponent(id)}/cancel`, runSchema, json({}));
  metrics = (id: string, signal: AbortSignal) =>
    this.request(`simulations/${encodeURIComponent(id)}/metrics`, metricsSchema, { signal });
  serverMetrics = (id: string, cursor: string | null, signal: AbortSignal) =>
    this.request(
      `simulations/${encodeURIComponent(id)}/metrics/servers?limit=32${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""}`,
      pageSchema(serverMetricSchema),
      { signal },
    );
  chart = (
    id: string,
    metric: Chart["metric"],
    start: number,
    end: number,
    server: string | null,
    signal: AbortSignal,
  ) =>
    this.request(
      `simulations/${encodeURIComponent(id)}/metrics?${new URLSearchParams({ metric, start_tick: String(start), end_tick: String(end), max_points: "500", ...(server ? { server_id: server } : {}) })}`,
      chartSchema.refine((value) => value.points.length <= 500),
      { signal },
    );
  compare = (ids: string[], signal: AbortSignal) =>
    this.request("simulations/compare", comparisonSchema, { ...json({ run_ids: ids }), signal });
  playback = (id: string, selection: PlaybackSelection, signal: AbortSignal) =>
    this.request(
      `simulations/${encodeURIComponent(id)}/playback?${new URLSearchParams({ tick: String(selection.tick), server_offset: String(selection.server_offset), request_kind: selection.request_kind, request_offset: String(selection.request_offset), ...(selection.server_id ? { server_id: selection.server_id } : {}) })}`,
      playbackSchema,
      { signal },
    );
  events = (id: string, tick: number, cursor: string | null, signal: AbortSignal) =>
    this.request(
      `simulations/${encodeURIComponent(id)}/events?${new URLSearchParams({ limit: "100", start_tick: String(tick), end_tick: String(tick), ...(cursor ? { cursor } : {}) })}`,
      pageSchema(eventSchema),
      { signal },
    );
}

export function json(body: unknown, method = "POST"): RequestInit {
  return { method, headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) };
}

/** getRandomValues also works on the internal HTTP browser-test origin. */
export function idempotencyKey() {
  return Array.from(crypto.getRandomValues(new Uint8Array(16)), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
}

import { describe, expect, it, vi } from "vitest";
import { createQueryClient } from "../queryClient.js";
import { Api, ApiError, boundedJson } from "./api.js";
import { defaultSettings, serverSchema, settingsSchema } from "./contracts.js";

describe("bounded same-origin transport", () => {
  it("holds admission through streamed consumption and aborts the response when the consumer finishes", async () => {
    const finish: (() => void)[] = [],
      signals: AbortSignal[] = [];
    const api = new Api("", async (_input, init) => {
      if (init?.signal) signals.push(init.signal);
      return new Response("stream");
    });
    const active = Array.from({ length: 8 }, () =>
      api.response(
        "simulations/example/stream",
        async () => new Promise<void>((resolve) => finish.push(resolve)),
      ),
    );
    await vi.waitFor(() => expect(finish).toHaveLength(8));
    await expect(
      api.response("simulations/example/stream", async () => undefined),
    ).rejects.toMatchObject({ code: "RATE_LIMITED" });
    expect(signals.every((signal) => !signal.aborted)).toBe(true);
    for (const resolve of finish) resolve();
    await Promise.all(active);
    expect(signals.every((signal) => signal.aborted)).toBe(true);
    await expect(
      api.response("simulations/example/stream", async () => undefined),
    ).resolves.toBeUndefined();
  });
  it("caps outstanding session requests and recovers after completed work", async () => {
    const waiting: ((response: Response) => void)[] = [];
    const api = new Api("", async () => new Promise<Response>((resolve) => waiting.push(resolve)));
    const requests = Array.from({ length: 8 }, () => api.servers(new AbortController().signal));
    await expect(api.servers(new AbortController().signal)).rejects.toMatchObject({
      code: "RATE_LIMITED",
    });
    expect(waiting).toHaveLength(8);
    for (const resolve of waiting) resolve(Response.json({ items: [], next_cursor: null }));
    await Promise.all(requests);
    const recovered = api.servers(new AbortController().signal);
    waiting[8]?.(Response.json({ items: [], next_cursor: null }));
    await expect(recovered).resolves.toMatchObject({ items: [] });
    api.close();
  });
  it("aborts in-flight mutations and erases its credential when the session closes", async () => {
    let signal: AbortSignal | null | undefined;
    const api = new Api("test-credential", async (_input, init) => {
      signal = init?.signal;
      return new Promise((_resolve, reject) => {
        signal?.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")));
      });
    });
    const pending = api.cancel("00000000-0000-0000-0000-000000000001");
    api.close();
    await expect(pending).rejects.toBeInstanceOf(ApiError);
    expect(signal?.aborted).toBe(true);
  });
  it("cancels an oversized body before parsing and does not echo its content", async () => {
    const cancelled = vi.fn();
    const response = new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new Uint8Array(1024 ** 2 + 1));
        },
        cancel: cancelled,
      }),
    );
    await expect(boundedJson(response)).rejects.toThrow("exceeds 1 MiB");
    expect(cancelled).toHaveBeenCalledOnce();
  });
  it("keeps bearer credentials in headers, validates results, and preserves expected versions", async () => {
    const server = { id: "a/b", cpu_units_per_tick: 1, mem_mb: 10, rate_limit_per_sec: 2 };
    const calls: { path: string; init: RequestInit }[] = [];
    const fetcher: typeof fetch = async (input, init) => {
      calls.push({ path: String(input), init: init ?? {} });
      return Response.json({ ...server, version: 2 });
    };
    const api = new Api("test-bearer-memory-only", fetcher);
    expect((await api.updateServer(server, 1)).version).toBe(2);
    expect(calls[0]?.path).toBe("/api/v1/servers/a%2Fb");
    const headers = new Headers(calls[0]?.init.headers);
    expect(headers.get("Authorization")).toBe("Bearer test-bearer-memory-only");
    expect(headers.get("If-Match")).toBe('"1"');
    expect(calls[0]?.init.redirect).toBe("error");
    expect(calls[0]?.init.credentials).toBe("omit");
    expect(JSON.stringify(calls[0]?.init.body)).not.toContain("test-bearer");
  });
  it("retains safe field errors but hides transport details", async () => {
    const api = new Api("", async () =>
      Response.json(
        {
          error: {
            code: "VALIDATION_ERROR",
            message: "CSV validation failed",
            details: [{ field: "row.2", reason: "invalid_integer" }],
          },
          request_id: "correlation-id",
        },
        { status: 422 },
      ),
    );
    await expect(api.upload(new File(["bad"], "bad.csv"))).rejects.toMatchObject({
      fields: [{ field: "row.2", reason: "invalid_integer" }],
      requestId: "correlation-id",
    });
    const unavailable = new Api("", async () => {
      throw new Error("private-token-should-not-render");
    });
    await expect(unavailable.servers(new AbortController().signal)).rejects.toThrow("Cannot reach");
    expect(
      await unavailable.servers(new AbortController().signal).catch((error) => String(error)),
    ).not.toContain("private-token");
  });
  it("rejects oversized uploads without invoking fetch and preserves caller cancellation", async () => {
    const fetcher = vi.fn<typeof fetch>();
    const api = new Api("", fetcher);
    await expect(
      api.upload(new File([new Uint8Array(10 * 1024 ** 2 + 1)], "large.csv")),
    ).rejects.toBeInstanceOf(ApiError);
    expect(fetcher).not.toHaveBeenCalled();
    const aborter = new AbortController();
    aborter.abort();
    const cancelled = new Api("", async (_input, init) => {
      init?.signal?.throwIfAborted();
      return Response.json({});
    });
    await expect(cancelled.servers(aborter.signal)).rejects.toMatchObject({ name: "AbortError" });
  });
});

describe("contracts and query cache", () => {
  it("rejects coerced/unknown settings and retains exact large fraction strings", () => {
    expect(settingsSchema.parse(defaultSettings)).toEqual(defaultSettings);
    expect(settingsSchema.safeParse({ ...defaultSettings, max_queue_size: "10" }).success).toBe(
      false,
    );
    expect(settingsSchema.safeParse({ ...defaultSettings, unexpected: true }).success).toBe(false);
    expect(
      serverSchema.safeParse({
        id: "server",
        version: 1,
        mem_mb: 0,
        cpu_units_per_tick: 1,
        rate_limit_per_sec: 1,
      }).success,
    ).toBe(false);
    expect(
      serverSchema.safeParse({
        id: "é".repeat(65),
        version: 1,
        mem_mb: 1,
        cpu_units_per_tick: 1,
        rate_limit_per_sec: 1,
      }).success,
    ).toBe(false);
    const large = structuredClone(defaultSettings);
    large.autoscaling.scale_out_cpu_threshold = {
      numerator: "1267650600228229401496703205377",
      denominator: "1267650600228229401496703205378",
    };
    expect(settingsSchema.parse(large).autoscaling.scale_out_cpu_threshold).toEqual(
      large.autoscaling.scale_out_cpu_threshold,
    );
  });
  it("evicts inactive run pages and aborts active reads on session clear", async () => {
    const client = createQueryClient();
    const aborter = vi.fn();
    const pending = client
      .fetchQuery({
        queryKey: ["run", "old-session"],
        queryFn: ({ signal }) =>
          new Promise((_resolve, reject) => {
            signal.addEventListener("abort", () => {
              aborter();
              reject(new Error("cancelled"));
            });
          }),
      })
      .catch(() => undefined);
    client.clear();
    await pending;
    expect(aborter).toHaveBeenCalledOnce();
    expect(client.getQueryCache().getAll()).toHaveLength(0);
    await client.fetchQuery({ queryKey: ["page", 1], queryFn: () => [1] });
    await new Promise((resolve) => setTimeout(resolve, 10));
    expect(client.getQueryCache().getAll()).toHaveLength(0);
    client.clear();
  });
});

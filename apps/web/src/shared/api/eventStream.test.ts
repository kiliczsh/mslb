import { afterEach, describe, expect, it, vi } from "vitest";
import { Api } from "./api.js";
import { defaultSettings, type Run } from "./contracts.js";
import { eventFrames, type LiveState, type TraceEvent, watchRun } from "./eventStream.js";

const run: Run = {
  id: "00000000-0000-4000-8000-000000000001",
  dataset_id: "00000000-0000-4000-8000-000000000002",
  snapshot_digest: "a".repeat(64),
  dataset_digest: "b".repeat(64),
  server_snapshot_digest: "c".repeat(64),
  engine_version: "0.2.0",
  configuration: defaultSettings,
  status: "running",
  attempts: 1,
  created_at: "2026-11-06T00:00:00Z",
  completed_at: null,
  error_code: null,
  progress: null,
};
function trace(sequence: number): TraceEvent {
  return {
    sequence,
    t: sequence,
    event: "REQUEST_ARRIVED",
    request_id: "é-😀",
    server_id: null,
    reason: null,
  };
}
function frame(sequence: number) {
  return `id: ${sequence}\nevent: trace\ndata: ${JSON.stringify(trace(sequence))}\n\n`;
}
function status() {
  const { configuration: _configuration, ...rest } = run;
  return `event: status\ndata: ${JSON.stringify({ ...rest, status: "completed", completed_at: "2026-11-06T00:00:01Z" })}\n\n`;
}
function stream(text: string) {
  return new Response(text, { headers: { "Content-Type": "text/event-stream" } });
}
async function collect(body: ReadableStream<Uint8Array>, signal = new AbortController().signal) {
  const result = [];
  for await (const frame of eventFrames(body, signal)) result.push(frame);
  return result;
}
afterEach(() => vi.useRealTimers());

describe("bounded fetch event delivery", () => {
  it("decodes split UTF-8 and CRLF, multiline data, comments and discards unterminated data", async () => {
    const bytes = new TextEncoder().encode(
      ": heartbeat\r\nevent: trace\rid: 1\ndata: é😀\r\ndata: tail\n\ndata: unfinished",
    );
    let index = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (index === bytes.length) controller.close();
        else controller.enqueue(bytes.subarray(index, ++index));
      },
    });
    expect(await collect(body)).toEqual([{ event: "trace", id: "1", data: "é😀\ntail" }]);
  });
  it("rejects oversized UTF-8 lines/frames and incomplete encoding and cancels its reader", async () => {
    for (const value of [
      `data: ${"é".repeat(32768)}`,
      `data: ${"a".repeat(40000)}\ndata: ${"b".repeat(40000)}\n\n`,
    ]) {
      const cancelled = vi.fn();
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode(value));
        },
        cancel: cancelled,
      });
      await expect(collect(body)).rejects.toThrow("64 KiB");
      expect(cancelled).toHaveBeenCalledOnce();
    }
    await expect(
      collect(
        new ReadableStream({
          start(controller) {
            controller.enqueue(new Uint8Array([0xc3]));
            controller.close();
          },
        }),
      ),
    ).rejects.toThrow();
  });
  it("cancels a blocked reader immediately when its owner aborts", async () => {
    const cancelled = vi.fn(),
      aborter = new AbortController();
    const result = collect(new ReadableStream({ cancel: cancelled }), aborter.signal);
    aborter.abort();
    await result;
    expect(cancelled).toHaveBeenCalledOnce();
  });
  it("reconnects with only persisted IDs, deduplicates replay, and ignores progress as a cursor", async () => {
    vi.useFakeTimers();
    const ids: (string | null)[] = [],
      updates: LiveState[] = [];
    const api = new Api("memory-token", async (_url, init) => {
      const headers = new Headers(init?.headers);
      expect(headers.get("Authorization")).toBe("Bearer memory-token");
      ids.push(headers.get("Last-Event-ID"));
      if (ids.length === 1) return stream(frame(1));
      const progress = {
        id: run.id,
        status: "running",
        attempts: 2,
        completed_at: null,
        error_code: null,
        progress: null,
      };
      return stream(
        `event: progress\ndata: ${JSON.stringify(progress)}\n\n${frame(1)}${frame(2)}${status()}`,
      );
    });
    const result = watchRun(api, run, (state) => updates.push(state), new AbortController().signal);
    await vi.advanceTimersByTimeAsync(1000);
    await result;
    expect(ids).toEqual(["0", "1"]);
    expect(updates.at(-1)).toMatchObject({
      mode: "complete",
      sequence: 2,
      events: [trace(1), trace(2)],
    });
    expect(vi.getTimerCount()).toBe(0);
  });
  it("falls back to paged persisted replay after three failures and drains the terminal tail", async () => {
    vi.useFakeTimers();
    let connections = 0;
    const cursors: (string | null)[] = [],
      updates: LiveState[] = [];
    const api = new Api("", async (input) => {
      const url = String(input);
      if (url.endsWith("/stream")) {
        connections += 1;
        return stream("");
      }
      if (url.includes("/events?")) {
        const cursor = new URL(url, "http://localhost").searchParams.get("cursor");
        cursors.push(cursor);
        return Response.json({
          items: cursor ? [trace(2)] : [trace(1)],
          next_cursor: cursor ? null : "page-two",
        });
      }
      return Response.json({
        ...run,
        status: "completed",
        mode: "concurrent",
        strategy: "first_fit",
        validation: null,
      });
    });
    const result = watchRun(api, run, (state) => updates.push(state), new AbortController().signal);
    await vi.advanceTimersByTimeAsync(2000);
    await result;
    expect(connections).toBe(3);
    expect(cursors).toEqual([null, "page-two", "page-two"]);
    expect(updates.some((state) => state.mode === "polling")).toBe(true);
    expect(updates.at(-1)).toMatchObject({
      mode: "complete",
      sequence: 2,
      events: [trace(1), trace(2)],
    });
    expect(vi.getTimerCount()).toBe(0);
  });
  it("keeps only a bounded recent view while advancing the durable cursor through a long trace", async () => {
    const updates: LiveState[] = [];
    const api = new Api("", async () =>
      stream(Array.from({ length: 5000 }, (_, index) => frame(index + 1)).join("") + status()),
    );
    await watchRun(api, run, (state) => updates.push(state), new AbortController().signal);
    expect(updates.at(-1)).toMatchObject({
      mode: "complete",
      sequence: 5000,
      droppedFromView: 4000,
    });
    expect(updates.at(-1)?.events).toHaveLength(1000);
    expect(updates.at(-1)?.events[0]?.sequence).toBe(4001);
  });
  it("does not accept gaps or operational IDs and stops retrying an unauthorized stream", async () => {
    vi.useFakeTimers();
    const updates: LiveState[] = [],
      headers: (string | null)[] = [];
    const api = new Api("", async (_input, init) => {
      headers.push(new Headers(init?.headers).get("Last-Event-ID"));
      if (headers.length === 1) return stream(frame(2));
      if (headers.length === 2)
        return stream(
          `${frame(1)}id: 99\nevent: progress\ndata: ${JSON.stringify({ id: run.id, status: "running", attempts: 1, completed_at: null, error_code: null, progress: null })}\n\n`,
        );
      return Response.json(
        {
          error: { code: "UNAUTHORIZED", message: "Authentication required", details: [] },
          request_id: "safe",
        },
        { status: 401 },
      );
    });
    const result = watchRun(api, run, (state) => updates.push(state), new AbortController().signal);
    await vi.advanceTimersByTimeAsync(2000);
    await result;
    expect(headers).toEqual(["0", "0", "1"]);
    expect(updates.at(-1)).toMatchObject({ mode: "unavailable", sequence: 1, events: [trace(1)] });
  });
  it("cancels reconnect timers and emits no further state after navigation", async () => {
    vi.useFakeTimers();
    const aborter = new AbortController(),
      updates: LiveState[] = [];
    const fetcher = vi.fn<typeof fetch>(async () => stream(""));
    const watching = watchRun(
      new Api("", fetcher),
      run,
      (state) => updates.push(state),
      aborter.signal,
    ).catch(() => undefined);
    await vi.advanceTimersByTimeAsync(100);
    aborter.abort();
    const count = updates.length;
    await vi.advanceTimersByTimeAsync(20_000);
    await watching;
    expect(fetcher).toHaveBeenCalledOnce();
    expect(updates).toHaveLength(count);
    expect(vi.getTimerCount()).toBe(0);
  });
});

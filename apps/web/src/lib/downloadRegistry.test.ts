import { createHash } from "node:crypto";
import { afterEach, describe, expect, it, vi } from "vitest";
import { DownloadRegistry } from "../../worker/registry.js";

afterEach(() => vi.useRealTimers());
describe("ephemeral browser download ownership and backpressure", () => {
  it("binds one-use transfers to the native client, caps active slots, and clears expired pending work", async () => {
    vi.useFakeTimers();
    const registry = new DownloadRegistry(),
      cancelled = vi.fn();
    const body = () => new ReadableStream<Uint8Array>({ cancel: cancelled });
    expect(registry.prepare("page-a", 1, body(), "run.jsonl", "application/x-ndjson")).toBe(true);
    expect(registry.prepare("page-b", 1, body(), "run.jsonl", "application/x-ndjson")).toBe(true);
    expect(registry.prepare("page-c", 1, body(), "run.jsonl", "application/x-ndjson")).toBe(false);
    expect(registry.take("page-c", 1).status).toBe(410);
    const response = registry.take("page-a", 1);
    expect(response.headers.get("Content-Disposition")).toContain("run.jsonl");
    expect(registry.take("page-a", 1).status).toBe(410);
    await registry.cancelOwner("page-a");
    await expect(response.text()).rejects.toThrow("cancelled");
    await vi.advanceTimersByTimeAsync(30_000);
    expect(registry.size).toBe(0);
    expect(cancelled).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(0);
  });
  it("expires an active transfer with an error instead of reporting a truncated success", async () => {
    vi.useFakeTimers();
    const registry = new DownloadRegistry(),
      cancelled = vi.fn(),
      notify = vi.fn();
    registry.prepare(
      "owner",
      1,
      new ReadableStream({ cancel: cancelled }),
      "run.jsonl",
      "application/x-ndjson",
      notify,
    );
    const reader = registry.take("owner", 1).body?.getReader();
    if (!reader) throw new Error("Response body is missing");
    const pending = reader.read();
    const failed = expect(pending).rejects.toThrow("cancelled");
    await vi.advanceTimersByTimeAsync(300_000);
    await failed;
    expect(registry.size).toBe(0);
    expect(cancelled).toHaveBeenCalledOnce();
    expect(notify).toHaveBeenCalledExactlyOnceWith(false);
  });
  it("streams 100 MiB through fixed-size chunks without accumulating the file and preserves its digest", async () => {
    const registry = new DownloadRegistry(),
      notify = vi.fn();
    const block = new Uint8Array(65536).fill(0x61),
      count = 1600;
    let produced = 0;
    registry.prepare(
      "owner",
      1,
      new ReadableStream({
        pull(controller) {
          if (produced === count) controller.close();
          else {
            produced += 1;
            controller.enqueue(block);
          }
        },
      }),
      "run.jsonl",
      "application/x-ndjson",
      notify,
    );
    const response = registry.take("owner", 1);
    await Promise.resolve();
    expect(produced).toBeLessThanOrEqual(3);
    const reader = response.body?.getReader();
    if (!reader) throw new Error("Response body is missing");
    const actual = createHash("sha256"),
      expected = createHash("sha256");
    let received = 0;
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      expect(value.byteLength).toBeLessThanOrEqual(65536);
      received += value.byteLength;
      actual.update(value);
    }
    for (let index = 0; index < count; index += 1) expected.update(block);
    expect(received).toBe(100 * 1024 ** 2);
    expect(actual.digest("hex")).toBe(expected.digest("hex"));
    expect(registry.size).toBe(0);
    expect(notify).toHaveBeenCalledExactlyOnceWith(true);
  });
  it("cancels upstream immediately on native download cancellation or an oversized chunk", async () => {
    const registry = new DownloadRegistry(),
      cancelled = vi.fn(),
      notify = vi.fn();
    registry.prepare(
      "owner",
      1,
      new ReadableStream({ cancel: cancelled }),
      "inputs.zip",
      "application/zip",
      notify,
    );
    await registry.take("owner", 1).body?.cancel();
    expect(cancelled).toHaveBeenCalledOnce();
    expect(notify).toHaveBeenCalledExactlyOnceWith(false);
    registry.prepare(
      "owner",
      2,
      new ReadableStream({
        start(controller) {
          controller.enqueue(new Uint8Array(65537));
        },
        cancel: cancelled,
      }),
      "inputs.zip",
      "application/zip",
    );
    await expect(registry.take("owner", 2).arrayBuffer()).rejects.toThrow("chunk exceeds");
    expect(cancelled).toHaveBeenCalledTimes(2);
    expect(registry.size).toBe(0);
  });
});

import type { z } from "zod";
import { type Api, ApiError } from "./api.js";
import { type Run, runSchema, terminal } from "./contracts.js";
import { eventSchema } from "./resultContracts.js";

export type TraceEvent = z.infer<typeof eventSchema>;
const progressSchema = runSchema.pick({
  id: true,
  status: true,
  attempts: true,
  completed_at: true,
  error_code: true,
  progress: true,
});
const finalSchema = runSchema.omit({ configuration: true });
export type LiveState = {
  mode: "connecting" | "live" | "reconnecting" | "polling" | "complete" | "unavailable";
  sequence: number;
  events: TraceEvent[];
  droppedFromView: number;
  status: Run["status"];
  progress: z.infer<typeof progressSchema>["progress"];
};
export type StreamFrame = { event: string; id: string | null; data: string };

/** Incremental UTF-8/CRLF parsing with bounded line/frame state across chunks. */
export async function* eventFrames(
  body: ReadableStream<Uint8Array>,
  signal: AbortSignal,
): AsyncGenerator<StreamFrame> {
  const reader = body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let line = "",
    data: string[] = [],
    size = 0,
    event = "message",
    id: string | null = null,
    cr = false;
  let lineBytes = 0;
  function consume(): StreamFrame | null {
    if (line === "") {
      const result = data.length ? { event, id, data: data.join("\n") } : null;
      data = [];
      size = 0;
      event = "message";
      id = null;
      return result;
    }
    size += lineBytes;
    if (size > 65536) throw new ApiError("Event frame exceeds 64 KiB.", "INVALID_RESPONSE");
    const split = line.indexOf(":");
    const name = split < 0 ? line : line.slice(0, split);
    let value = split < 0 ? "" : line.slice(split + 1);
    if (value.startsWith(" ")) value = value.slice(1);
    if (name === "data") data.push(value);
    if (name === "event") event = value;
    if (name === "id") id = value;
    return null;
  }
  const abort = () => {
    void reader.cancel().catch(() => undefined);
  };
  signal.addEventListener("abort", abort, { once: true });
  try {
    while (true) {
      signal.throwIfAborted();
      const { done, value } = await reader.read();
      if (done) {
        decoder.decode();
        return;
      }
      if (value.byteLength > 1024 ** 2)
        throw new ApiError("Event chunk exceeds its byte budget.", "INVALID_RESPONSE");
      const text = decoder.decode(value, { stream: true });
      for (const character of text) {
        if (cr && character === "\n") {
          cr = false;
          continue;
        }
        cr = character === "\r";
        if (character === "\n" || character === "\r") {
          const frame = consume();
          line = "";
          lineBytes = 0;
          if (frame) yield frame;
        } else {
          line += character;
          const code = character.codePointAt(0) ?? 0;
          lineBytes += code <= 0x7f ? 1 : code <= 0x7ff ? 2 : code <= 0xffff ? 3 : 4;
          if (lineBytes > 65536)
            throw new ApiError("Event line exceeds 64 KiB.", "INVALID_RESPONSE");
        }
      }
    }
  } finally {
    signal.removeEventListener("abort", abort);
    await reader.cancel();
    reader.releaseLock();
  }
}

export function sleepUntil(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    signal.throwIfAborted();
    const abort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve();
    }, ms);
    signal.addEventListener("abort", abort, { once: true });
  });
}

/** One connection/page at a time. Cursor state survives transport changes, never progress IDs. */
export async function watchRun(
  api: Api,
  run: Run,
  observe: (state: LiveState) => void,
  signal: AbortSignal,
): Promise<void> {
  const state: LiveState = {
    mode: "connecting",
    sequence: 0,
    events: [],
    droppedFromView: 0,
    status: run.status,
    progress: run.progress,
  };
  let failures = 0,
    cursor: string | null = null,
    retainedBytes = 0;
  let notification: ReturnType<typeof setTimeout> | null = null;
  const encoder = new TextEncoder();
  function emit(immediate = false) {
    if (signal.aborted) return;
    if (immediate && notification) {
      clearTimeout(notification);
      notification = null;
    }
    if (immediate) observe({ ...state, events: [...state.events] });
    else if (!notification)
      notification = setTimeout(() => {
        notification = null;
        emit(true);
      }, 50);
  }
  function append(value: TraceEvent) {
    if (value.sequence <= state.sequence) return;
    if (value.sequence !== state.sequence + 1)
      throw new ApiError("Stored event replay has a gap.", "REPLAY_REQUIRED");
    state.sequence = value.sequence;
    state.events.push(value);
    retainedBytes += encoder.encode(JSON.stringify(value)).byteLength;
    while (state.events.length > 1000 || retainedBytes > 512 * 1024) {
      const first = state.events.shift();
      if (first) retainedBytes -= encoder.encode(JSON.stringify(first)).byteLength;
      state.droppedFromView += 1;
    }
  }
  emit(true);
  try {
    while (!signal.aborted) {
      const started = Date.now();
      try {
        state.mode = failures === 0 ? "connecting" : "reconnecting";
        emit(true);
        let ended = false;
        await api.response(
          `simulations/${encodeURIComponent(run.id)}/stream`,
          async (response, connection) => {
            if (
              !response.body ||
              !response.headers.get("content-type")?.startsWith("text/event-stream")
            )
              throw new ApiError("The event transport is unavailable.", "INVALID_RESPONSE");
            state.mode = "live";
            emit(true);
            for await (const frame of eventFrames(response.body, connection)) {
              if (frame.event === "trace") {
                const event = eventSchema.parse(JSON.parse(frame.data));
                if (frame.id !== String(event.sequence))
                  throw new ApiError("Invalid persisted event identifier.", "INVALID_RESPONSE");
                append(event);
                emit();
              } else if (frame.event === "progress") {
                const progress = progressSchema.parse(JSON.parse(frame.data));
                if (progress.id !== run.id || frame.id !== null)
                  throw new ApiError("Invalid operational progress context.", "INVALID_RESPONSE");
                state.status = progress.status;
                state.progress = progress.progress;
                emit();
              } else if (frame.event === "status") {
                const status = finalSchema.parse(JSON.parse(frame.data));
                if (status.id !== run.id || !terminal(status.status) || frame.id !== null)
                  throw new ApiError("Invalid terminal context.", "INVALID_RESPONSE");
                state.status = status.status;
                state.progress = status.progress;
                ended = true;
                break;
              } else if (["error", "replay_required"].includes(frame.event)) {
                throw new ApiError(
                  "The event connection needs persisted replay.",
                  "REPLAY_REQUIRED",
                );
              }
            }
          },
          {
            headers: { Accept: "text/event-stream", "Last-Event-ID": String(state.sequence) },
            signal,
          },
        );
        if (ended) {
          state.mode = "complete";
          emit(true);
          return;
        }
        throw new ApiError("The event connection was interrupted.");
      } catch (error) {
        if (signal.aborted) return;
        if (error instanceof ApiError && ["UNAUTHORIZED", "FORBIDDEN"].includes(error.code)) {
          state.mode = "unavailable";
          emit(true);
          return;
        }
        if (Date.now() - started >= 5000) failures = 0;
        failures += 1;
        state.mode = "reconnecting";
        emit(true);
        if (failures < 3) {
          await sleepUntil(Math.min(4000, 250 * 2 ** failures), signal);
          continue;
        }
      }
      state.mode = "polling";
      emit(true);
      const retryAt = Date.now() + 10_000;
      while (!signal.aborted && Date.now() < retryAt) {
        try {
          const page = await api.eventHistory(run.id, cursor, signal);
          for (const event of page.items) append(event);
          if (page.next_cursor) cursor = page.next_cursor;
          emit();
          if (page.next_cursor) continue;
          const status = await api.run(run.id, signal);
          state.status = status.status;
          state.progress = status.progress;
          // Re-read once after terminal metadata to cover a racing final event commit.
          if (terminal(status.status)) {
            const tail = await api.eventHistory(run.id, cursor, signal);
            for (const event of tail.items) append(event);
            if (tail.next_cursor) {
              cursor = tail.next_cursor;
              continue;
            }
            state.mode = "complete";
            emit(true);
            return;
          }
        } catch (error) {
          if (signal.aborted) return;
          if (error instanceof ApiError && ["UNAUTHORIZED", "FORBIDDEN"].includes(error.code)) {
            state.mode = "unavailable";
            emit(true);
            return;
          }
          cursor = null;
          state.mode = "unavailable";
          emit(true);
        }
        await sleepUntil(2000, signal);
      }
      failures = 2;
    }
  } finally {
    if (notification) clearTimeout(notification);
  }
}

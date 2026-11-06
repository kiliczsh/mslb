import { ApiError } from "../api/api.js";

let nextId = 0;
export const artifactTypes = {
  "run.jsonl": "application/x-ndjson",
  "timeline.jsonl": "application/x-ndjson",
  "autoscaling_decisions.jsonl": "application/x-ndjson",
  "drop_reasons.jsonl": "application/x-ndjson",
  "metrics.json": "application/json",
  "manifest.json": "application/json",
  "snapshot.json": "application/json",
  "servers.json": "application/json",
  "requests.csv": "text/csv",
  "inputs.zip": "application/zip",
} as const;
export type Artifact = keyof typeof artifactTypes;
export function isArtifact(value: string): value is Artifact {
  return Object.hasOwn(artifactTypes, value);
}

function setup<T>(operation: Promise<T>, signal: AbortSignal): Promise<T> {
  signal.throwIfAborted();
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", abort);
    };
    const abort = () => {
      cleanup();
      reject(new ApiError("Download setup interrupted. Retry from the saved result."));
    };
    const timer = setTimeout(abort, 10_000);
    signal.addEventListener("abort", abort, { once: true });
    operation.then(
      (value) => {
        cleanup();
        resolve(value);
      },
      () => {
        cleanup();
        reject(new ApiError("Download support is unavailable. Retry from the saved result."));
      },
    );
  });
}

async function controller(signal: AbortSignal) {
  if (!globalThis.isSecureContext || !("serviceWorker" in navigator))
    throw new ApiError(
      "Open this workspace on localhost or HTTPS to stream downloads.",
      "UNAVAILABLE",
    );
  await setup(
    navigator.serviceWorker.register("/download-worker.js", {
      type: "module",
      updateViaCache: "none",
    }),
    signal,
  );
  await setup(navigator.serviceWorker.ready, signal);
  if (!navigator.serviceWorker.controller)
    await new Promise<void>((resolve, reject) => {
      const complete = () => {
        clearTimeout(timeout);
        signal.removeEventListener("abort", abort);
        navigator.serviceWorker.removeEventListener("controllerchange", complete);
        resolve();
      };
      const abort = () => {
        clearTimeout(timeout);
        signal.removeEventListener("abort", abort);
        navigator.serviceWorker.removeEventListener("controllerchange", complete);
        reject(new ApiError("Download setup interrupted."));
      };
      const timeout = setTimeout(abort, 10_000);
      navigator.serviceWorker.addEventListener("controllerchange", complete, { once: true });
      signal.addEventListener("abort", abort, { once: true });
    });
  signal.throwIfAborted();
  const active = navigator.serviceWorker.controller;
  if (!active) throw new ApiError("Download support is not ready. Retry the download.");
  return active;
}

/** Transfer a backpressured authenticated response, never a bearer token, to browser download. */
export async function browserDownload(
  response: Response,
  signal: AbortSignal,
  filename: Artifact,
  progress: (bytes: number) => void,
) {
  const media = response.headers.get("content-type")?.split(";")[0];
  if (media !== artifactTypes[filename] || !response.body)
    throw new ApiError(
      "The artifact response does not match its expected format.",
      "INVALID_RESPONSE",
    );
  const worker = await controller(signal);
  const id = ++nextId;
  const source = response.body.getReader();
  const expectedHeader = response.headers.get("content-length");
  const expected = expectedHeader === null ? null : Number(expectedHeader);
  if (
    expected !== null &&
    (!Number.isSafeInteger(expected) || expected < 0 || expected > 512 * 1024 ** 2)
  ) {
    await source.cancel();
    throw new ApiError("Artifact length exceeds its supported limit.", "INVALID_RESPONSE");
  }
  let transferred = 0,
    pending: Uint8Array | null = null,
    offset = 0;
  let resolveDone: () => void = () => undefined,
    rejectDone: (reason: unknown) => void = () => undefined;
  const done = new Promise<void>((resolve, reject) => {
    resolveDone = resolve;
    rejectDone = reject;
  });
  // Attach a rejection handler before transferring: a cancelled consumer can fail before the ready ACK.
  const settled = done.then(
    () => ({ error: null }),
    () => ({ error: new ApiError("Download interrupted. Retry from the saved result.") }),
  );
  const body = new ReadableStream<Uint8Array>({
    pull: async (target) => {
      try {
        signal.throwIfAborted();
        if (!pending || offset >= pending.length) {
          const chunk = await source.read();
          if (chunk.done) {
            if (expected !== null && transferred !== expected)
              throw new ApiError("Artifact length changed during transfer.", "INVALID_RESPONSE");
            target.close();
            resolveDone();
            return;
          }
          if (chunk.value.byteLength > 1024 ** 2)
            throw new ApiError("Artifact chunk exceeds its byte budget.", "INVALID_RESPONSE");
          pending = chunk.value;
          offset = 0;
        }
        const part = pending.slice(offset, offset + 65536);
        offset += part.length;
        transferred += part.length;
        if (transferred > 512 * 1024 ** 2 || (expected !== null && transferred > expected))
          throw new ApiError("Artifact exceeds its transfer budget.", "INVALID_RESPONSE");
        target.enqueue(part);
        progress(transferred);
      } catch {
        target.error(new Error("Artifact transfer interrupted"));
        rejectDone(new ApiError("Artifact transfer interrupted"));
        await source.cancel().catch(() => undefined);
      }
    },
    cancel: async () => {
      rejectDone(new ApiError("Download cancelled"));
      await source.cancel().catch(() => undefined);
    },
  });
  const channel = new MessageChannel();
  let ready: () => void = () => undefined,
    failReady: (error: unknown) => void = () => undefined;
  let completed: () => void = () => undefined,
    failComplete: (error: unknown) => void = () => undefined;
  const readiness = new Promise<void>((resolve, reject) => {
    ready = resolve;
    failReady = reject;
  });
  const completion = new Promise<void>((resolve, reject) => {
    completed = resolve;
    failComplete = reject;
  }).then(
    () => null,
    () => new ApiError("Download interrupted. Retry from the saved result."),
  );
  const abort = () => {
    worker.postMessage({ kind: "cancel", id });
    const error = new ApiError("Download cancelled");
    rejectDone(error);
    failReady(error);
    failComplete(error);
    void source.cancel().catch(() => undefined);
  };
  signal.addEventListener("abort", abort, { once: true });
  const timer = setTimeout(() => {
    failReady(new ApiError("Download setup did not complete. Retry."));
  }, 10_000);
  try {
    channel.port1.onmessage = (event) => {
      const value: unknown = event.data;
      if (
        !value ||
        typeof value !== "object" ||
        !("kind" in value) ||
        !("id" in value) ||
        value.id !== id
      )
        return;
      if (value.kind === "ready") {
        clearTimeout(timer);
        ready();
      } else if (value.kind === "complete") completed();
      else {
        const error = new ApiError(
          "Browser download is unavailable or busy. Retry after active transfers.",
          "RATE_LIMITED",
        );
        failReady(error);
        failComplete(error);
      }
    };
    worker.postMessage({ kind: "prepare", id, body, filename, media }, [body, channel.port2]);
    await readiness;
    signal.throwIfAborted();
    const link = document.createElement("a");
    link.href = `/_download/${id}`;
    // Navigation reaches the worker; a download-attribute request can bypass it in Chromium.
    // The attachment response starts a download without replacing the workspace document.
    document.body.append(link);
    link.click();
    link.remove();
    const completeError = await completion;
    if (completeError) throw completeError;
    const result = await settled;
    if (result.error) throw result.error;
  } finally {
    clearTimeout(timer);
    signal.removeEventListener("abort", abort);
    worker.postMessage({ kind: "cancel", id });
    channel.port1.close();
    channel.port2.close();
    await source.cancel().catch(() => undefined);
    source.releaseLock();
  }
}

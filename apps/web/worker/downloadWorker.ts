import { DownloadRegistry } from "./registry.js";

// This entry point executes only as a service worker; its compiler uses WebWorker globals.
declare const self: ServiceWorkerGlobalScope;
const registry = new DownloadRegistry();
const mediaTypes = new Set([
  "application/json",
  "application/x-ndjson",
  "application/zip",
  "text/csv",
]);

self.addEventListener("install", (event) => {
  event.waitUntil(self.skipWaiting());
});
self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});
self.addEventListener("message", (event) => {
  event.waitUntil(
    (async () => {
      const source = event.source;
      if (!source || !("id" in source) || new URL(source.url).origin !== self.location.origin)
        return;
      const data: unknown = event.data;
      if (
        !data ||
        typeof data !== "object" ||
        !("kind" in data) ||
        !("id" in data) ||
        typeof data.id !== "number" ||
        !Number.isSafeInteger(data.id) ||
        data.id < 1
      )
        return;
      if (data.kind === "cancel") {
        await registry.cancel(source.id, data.id);
        return;
      }
      if (
        data.kind !== "prepare" ||
        !("body" in data) ||
        !(data.body instanceof ReadableStream) ||
        !("filename" in data) ||
        typeof data.filename !== "string" ||
        !/^[a-z][a-z0-9._-]{0,63}$/.test(data.filename) ||
        !("media" in data) ||
        typeof data.media !== "string" ||
        !mediaTypes.has(data.media)
      )
        return;
      for (const owner of registry.owners())
        if (!(await self.clients.get(owner))) await registry.cancelOwner(owner);
      const accepted = registry.prepare(
        source.id,
        data.id,
        data.body,
        data.filename,
        data.media,
        (success) =>
          event.ports[0]?.postMessage({ kind: success ? "complete" : "failed", id: data.id }),
      );
      event.ports[0]?.postMessage({ kind: accepted ? "ready" : "busy", id: data.id });
      if (!accepted) await data.body.cancel();
    })(),
  );
});
self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (url.origin !== self.location.origin || !url.pathname.startsWith("/_download/")) return;
  const id = Number(url.pathname.slice("/_download/".length));
  const owner = event.clientId;
  if (
    event.request.method !== "GET" ||
    url.search ||
    !owner ||
    !Number.isSafeInteger(id) ||
    id < 1
  ) {
    event.respondWith(new Response("Download requires its originating page.", { status: 403 }));
    return;
  }
  event.respondWith(registry.take(owner, id));
});

type Entry = {
  owner: string;
  id: number;
  reader: ReadableStreamDefaultReader<Uint8Array>;
  filename: string;
  media: string;
  active: boolean;
  timer: ReturnType<typeof setTimeout>;
  notify: (success: boolean) => void;
  controller: ReadableStreamDefaultController<Uint8Array> | null;
};

/** Credentials never enter this registry. IDs correlate streams; the native client ID owns access. */
export class DownloadRegistry {
  private entries = new Map<string, Entry>();
  private key(owner: string, id: number) {
    return `${owner}:${id}`;
  }
  get size() {
    return this.entries.size;
  }
  owners() {
    return [...new Set([...this.entries.values()].map((entry) => entry.owner))];
  }
  prepare(
    owner: string,
    id: number,
    body: ReadableStream<Uint8Array>,
    filename: string,
    media: string,
    notify: (success: boolean) => void = () => undefined,
  ) {
    const key = this.key(owner, id);
    if (this.entries.size >= 2 || this.entries.has(key)) return false;
    const reader = body.getReader();
    const timer = setTimeout(() => {
      void this.cancel(owner, id);
    }, 30_000);
    this.entries.set(key, {
      owner,
      id,
      reader,
      filename,
      media,
      active: false,
      timer,
      notify,
      controller: null,
    });
    return true;
  }
  async cancel(owner: string, id: number) {
    const key = this.key(owner, id),
      entry = this.entries.get(key);
    if (!entry) return;
    this.entries.delete(key);
    clearTimeout(entry.timer);
    entry.controller?.error(new Error("Download cancelled"));
    entry.notify(false);
    await entry.reader.cancel().catch(() => undefined);
  }
  async cancelOwner(owner: string) {
    await Promise.all(
      [...this.entries.values()]
        .filter((entry) => entry.owner === owner)
        .map((entry) => this.cancel(owner, entry.id)),
    );
  }
  take(owner: string, id: number): Response {
    const entry = this.entries.get(this.key(owner, id));
    if (!entry || entry.active)
      return new Response("Download is not available to this page.", { status: 410 });
    entry.active = true;
    clearTimeout(entry.timer);
    entry.timer = setTimeout(() => {
      void this.cancel(owner, id);
    }, 300_000);
    const finish = (success: boolean) => {
      if (!this.entries.delete(this.key(owner, id))) return;
      clearTimeout(entry.timer);
      entry.notify(success);
    };
    const stream = new ReadableStream<Uint8Array>({
      start: (controller) => {
        entry.controller = controller;
      },
      pull: async (controller) => {
        try {
          const { done, value } = await entry.reader.read();
          if (!this.entries.has(this.key(owner, id))) return;
          if (done) {
            controller.close();
            finish(true);
          } else if (!(value instanceof Uint8Array) || value.byteLength > 65536) {
            finish(false);
            controller.error(new Error("Download chunk exceeds its limit"));
            await entry.reader.cancel();
          } else controller.enqueue(value);
        } catch {
          finish(false);
          controller.error(new Error("Download interrupted"));
        }
      },
      cancel: async () => {
        finish(false);
        await entry.reader.cancel().catch(() => undefined);
      },
    });
    return new Response(stream, {
      headers: {
        "Content-Type": entry.media,
        "Content-Disposition": `attachment; filename="${entry.filename}"`,
        "Cache-Control": "no-store",
        "Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "no-referrer",
      },
    });
  }
}

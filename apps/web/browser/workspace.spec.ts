import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { readFile } from "node:fs/promises";
import { expect, test } from "@playwright/test";
import { z } from "zod";
import { acceptedSchema, detailSchema, serverSchema } from "../src/lib/contracts.js";

const fixtureHeaders = {
  Authorization: `Bearer ${process.env.AUTH_TOKEN ?? ""}`,
};
const fixtureServers = [
  { id: "s1", cpu_units_per_tick: 10, mem_mb: 1024, rate_limit_per_sec: 2 },
  { id: "s2", cpu_units_per_tick: 5, mem_mb: 512, rate_limit_per_sec: 1 },
];

test.beforeAll(async ({ request }) => {
  if (process.env.BROWSER_TEST_INSTALLATION !== "isolated")
    throw new Error("Refusing to prepare servers outside the disposable browser installation.");
  // Verify a fresh installation before changing any server. The runner destroys
  // this project's private volumes in finally, including when a test fails.
  for (const collection of ["simulations", "datasets"]) {
    const response = await request.get(`/api/v1/${collection}`, { headers: fixtureHeaders });
    expect(response.status()).toBe(200);
    expect(
      z.object({ items: z.array(z.unknown()), next_cursor: z.null() }).parse(await response.json())
        .items,
    ).toEqual([]);
  }
  const existing = await request.get("/api/v1/servers", { headers: fixtureHeaders });
  expect(existing.status()).toBe(200);
  const page = z
    .object({ items: z.array(serverSchema), next_cursor: z.null() })
    .parse(await existing.json());
  for (const server of page.items) {
    const response = await request.delete(`/api/v1/servers/${encodeURIComponent(server.id)}`, {
      headers: { ...fixtureHeaders, "If-Match": `"${server.version}"` },
    });
    expect(response.status()).toBe(204);
  }
  for (const server of fixtureServers) {
    const response = await request.post("/api/v1/servers", {
      headers: fixtureHeaders,
      data: server,
    });
    expect(response.status()).toBe(201);
    expect(serverSchema.parse(await response.json())).toMatchObject(server);
  }
  const prepared = await request.get("/api/v1/servers", { headers: fixtureHeaders });
  expect(
    z
      .object({ items: z.array(serverSchema) })
      .parse(await prepared.json())
      .items.map(({ version, ...server }) => server),
  ).toEqual(fixtureServers);
});

test("gateway serves built assets with isolation headers and rejects untrusted hosts/origins", async ({
  request,
}) => {
  const response = await request.get("/");
  expect(response.status()).toBe(200);
  expect(response.headers()["content-security-policy"]).toContain("connect-src 'self'");
  expect(response.headers()["x-content-type-options"]).toBe("nosniff");
  expect(
    (
      await request.get("/api/v1/servers", { headers: { Host: "untrusted.example:8080" } })
    ).status(),
  ).toBe(421);
  expect(
    (
      await request.get("/api/v1/servers", { headers: { Origin: "https://untrusted.example" } })
    ).status(),
  ).toBe(403);
});

test("authentication headers, visible denial and session reset without persistent credentials", async ({
  page,
}) => {
  const token = "browser_test_credential_12345678901234567890";
  await page.route("**/api/v1/**", async (route) => {
    if (route.request().headers().authorization !== `Bearer ${token}`) {
      await route.fulfill({
        status: 401,
        contentType: "application/json",
        body: JSON.stringify({
          error: { code: "UNAUTHORIZED", message: "Authentication required", details: [] },
          request_id: "browser-auth-test",
        }),
      });
    } else if (route.request().url().includes("health/ready")) {
      await route.fulfill({
        status: 503,
        contentType: "application/json",
        body: JSON.stringify({
          ready: false,
          api_ready: true,
          complete_product: false,
          capabilities: { dashboard: "incomplete" },
        }),
      });
    } else {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({ items: [], next_cursor: null }),
      });
    }
  });
  await page.goto("/");
  await expect(page.getByRole("alert").first()).toContainText("Authentication required");
  await page.getByLabel("Bearer token (authenticated mode)").fill(token);
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await expect(page.getByText("No servers configured.", { exact: false })).toBeVisible();
  await expect(page.getByLabel("Bearer token (authenticated mode)")).toHaveValue("");
  expect(
    await page.evaluate(() => ({ local: localStorage.length, session: sessionStorage.length })),
  ).toEqual({ local: 0, session: 0 });
  await page.getByRole("button", { name: "Clear session", exact: true }).click();
  await expect(page.getByRole("alert").first()).toContainText("Authentication required");
});

test("native streamed downloads remain one-use and bound to their originating browser page", async ({
  page,
  context,
}) => {
  await page.goto("/");
  await page.evaluate(async () => {
    await navigator.serviceWorker.register("/download-worker.js", { type: "module" });
    await navigator.serviceWorker.ready;
    if (!navigator.serviceWorker.controller)
      await new Promise<void>((resolve) =>
        navigator.serviceWorker.addEventListener("controllerchange", () => resolve(), {
          once: true,
        }),
      );
    const worker = navigator.serviceWorker.controller;
    if (!worker) throw new Error("Download worker is missing");
    let produced = 0;
    const body = new ReadableStream<Uint8Array>({
      pull(controller) {
        if (produced === 512) controller.close();
        else {
          produced += 1;
          controller.enqueue(new Uint8Array(65536).fill(0x61));
        }
      },
    });
    const channel = new MessageChannel();
    const ready = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error("Download worker did not acknowledge")),
        10_000,
      );
      channel.port1.onmessage = (event) => {
        clearTimeout(timer);
        if (event.data.kind === "ready") resolve();
        else reject(new Error("Download worker rejected its transfer"));
      };
    });
    worker.postMessage(
      { kind: "prepare", id: 9981, body, filename: "bounded.jsonl", media: "application/x-ndjson" },
      [body, channel.port2],
    );
    await ready;
    channel.port1.close();
  });
  const other = await context.newPage();
  await other.goto("/");
  expect(await other.evaluate(async () => (await fetch("/_download/9981")).status)).toBe(410);
  await other.close();
  const downloadReady = page.waitForEvent("download");
  await page.evaluate(() => {
    const link = document.createElement("a");
    link.href = "/_download/9981";
    document.body.append(link);
    link.click();
    link.remove();
  });
  const download = await downloadReady;
  expect(await download.failure()).toBeNull();
  expect(download.suggestedFilename()).toBe("bounded.jsonl");
  const path = await download.path();
  if (!path) throw new Error("Download path is missing");
  const actual = createHash("sha256"),
    expected = createHash("sha256");
  let size = 0;
  for await (const chunk of createReadStream(path)) {
    actual.update(chunk);
    size += chunk.byteLength;
  }
  for (let index = 0; index < 512; index += 1) expected.update(new Uint8Array(65536).fill(0x61));
  expect(size).toBe(32 * 1024 ** 2);
  expect(actual.digest("hex")).toBe(expected.digest("hex"));
  expect(await page.evaluate(async () => (await fetch("/_download/9981")).status)).toBe(410);
  expect(await page.evaluate(async () => await caches.keys())).toEqual([]);
});

test("real gateway CRUD, CSV, queued worker completion and narrow-screen session reset", async ({
  page,
}) => {
  // Two durable jobs include queueing, fsync and independent replay in the
  // disposable installation with the exact server configuration above.
  test.setTimeout(90_000);
  const completionTimeout = 30_000;
  const pageErrors: string[] = [];
  const diagnostics: string[] = [];
  const authenticated = process.env.AUTH_ENABLED === "true";
  let checkingDenial = authenticated;
  const denials: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("console", (message) => {
    const readinessDiagnostic =
      message.type() === "error" &&
      message.location().url.endsWith("/api/v1/health/ready") &&
      message.text().includes("503");
    const expectedDenial =
      checkingDenial &&
      message.type() === "error" &&
      message.location().url.endsWith("/api/v1/servers?limit=64") &&
      message.text().includes("401");
    if (expectedDenial) denials.push(message.text());
    else if ((message.type() === "warning" || message.type() === "error") && !readinessDiagnostic)
      diagnostics.push(message.text());
  });
  await page.goto("/");
  if (authenticated) {
    await expect(page.getByRole("alert").first()).toContainText("UNAUTHORIZED");
    await expect.poll(() => denials.length).toBe(1);
    checkingDenial = false;
    await page.getByLabel("Bearer token (authenticated mode)").fill(process.env.AUTH_TOKEN ?? "");
    await page.getByRole("button", { name: "Connect", exact: true }).click();
  }
  await expect(page.getByRole("heading", { name: "Server configuration" })).toBeVisible();
  const name = `browser-${crypto.randomUUID().slice(0, 8)}`;
  await page.getByLabel("Server ID", { exact: true }).fill(name);
  const createdResponse = page.waitForResponse(
    (response) =>
      response.url().endsWith("/api/v1/servers") && response.request().method() === "POST",
  );
  await page.getByRole("button", { name: "Create server", exact: true }).click();
  expect(pageErrors).toEqual([]);
  expect(await page.getByRole("alert").allTextContents()).toEqual([]);
  const created = await createdResponse;
  expect(created.status(), await created.text()).toBe(201);
  await expect(page.getByRole("button", { name: `Edit ${name}`, exact: true })).toBeVisible();
  await page.getByRole("button", { name: `Edit ${name}`, exact: true }).click();
  await page.getByLabel("Memory capacity (MB)").fill("2048");
  await page.getByRole("button", { name: "Save changes", exact: true }).click();
  await expect(page.getByRole("row", { name: new RegExp(name) })).toContainText("2048");
  await page.getByRole("button", { name: `Delete ${name}`, exact: true }).click();
  await expect(page.getByRole("alertdialog")).toContainText("Existing run snapshots are preserved");
  await page.getByRole("button", { name: "Confirm deletion", exact: true }).click();
  await expect(page.getByRole("button", { name: `Edit ${name}`, exact: true })).toHaveCount(0);
  await page.getByRole("button", { name: "Datasets", exact: true }).click();
  await page.getByLabel("Requests CSV").setInputFiles({
    name: "browser-requests.csv",
    mimeType: "text/csv",
    buffer: Buffer.from("t,request_id,work_units,mem_mb\n0,browser-a,2,1\n1,browser-b,1,1\n"),
  });
  await page.getByRole("button", { name: "Upload dataset", exact: true }).click();
  await expect(page.getByRole("status")).toContainText("registered and selected");
  await page.getByRole("button", { name: "Simulations", exact: true }).click();
  await expect(page.getByLabel("Execution mode")).toHaveValue("sequential");
  await page.getByRole("button", { name: "Review snapshot", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Review snapshot inputs" })).toBeVisible();
  const firstAccepted = page.waitForResponse(
    (response) =>
      response.url().endsWith("/api/v1/simulations") && response.request().method() === "POST",
  );
  await page.getByRole("button", { name: "Confirm and queue run", exact: true }).click();
  const firstResponse = await firstAccepted;
  const firstRun = acceptedSchema.parse(await firstResponse.json());
  expect(firstResponse.request().postDataJSON().mode).toBe("sequential");
  const creation = z.object({ dataset_id: z.uuid() }).parse(firstResponse.request().postDataJSON());
  expect(await page.getByRole("alert").allTextContents()).toEqual([]);
  await expect(page.getByText("Independent replay: passed.", { exact: false })).toBeVisible({
    timeout: completionTimeout,
  });
  await expect(page.getByRole("button", { name: "Copy settings for a new run" })).toBeVisible();
  await page.getByText("Reproducibility and saved configuration", { exact: true }).click();
  await expect(page.getByText(/^Snapshot: [a-f0-9]{64}$/)).toBeVisible();
  await expect(page.getByRole("heading", { name: "Exact completed metrics" })).toBeVisible();
  await page.getByRole("button", { name: "Inspect exact chart buckets" }).click();
  await expect(page.getByRole("table", { name: /^Exact chart buckets/ })).toContainText("2/15");
  await page.getByRole("button", { name: "Playback", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Playback from saved intervals" })).toBeVisible();
  await expect(page.getByText("Saved resource interval [0, 1)", { exact: false })).toBeVisible();
  await page.getByRole("button", { name: "Inspect active on s1", exact: true }).click();
  await expect(page.getByRole("list", { name: "Requests at selected interval" })).toContainText(
    "browser-a",
  );
  await page.getByRole("button", { name: "Next tick", exact: true }).click();
  await expect(page.getByText("Selected tick 1 of 2", { exact: false })).toBeVisible();
  await expect(page.getByRole("list", { name: "Events at selected tick" })).toContainText(
    "FINISHED",
  );
  await page.getByRole("button", { name: "Play playback", exact: true }).click();
  await expect(page.getByText("Terminal boundary", { exact: false })).toBeVisible();
  await page.getByRole("button", { name: "Comparison", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Compare compatible completed runs" }),
  ).toBeVisible();
  const headers =
    process.env.AUTH_ENABLED === "true"
      ? { Authorization: `Bearer ${process.env.AUTH_TOKEN ?? ""}` }
      : {};
  const secondResponse = await page.request.post("/api/v1/simulations", {
    headers,
    data: { dataset_id: creation.dataset_id, strategy: "round_robin" },
  });
  expect(secondResponse.status()).toBe(202);
  const second = acceptedSchema.parse(await secondResponse.json());
  await expect
    .poll(
      async () =>
        detailSchema.parse(
          await (
            await page.request.get(`/api/v1/simulations/${second.run_id}`, { headers })
          ).json(),
        ).status,
      { timeout: completionTimeout },
    )
    .toBe("completed");
  await page.getByLabel("Saved comparison run ID").fill(second.run_id);
  await page.getByRole("button", { name: "Add saved run", exact: true }).click();
  await expect(page.getByText("2 selected across history pages.", { exact: false })).toBeVisible();
  await page.getByRole("button", { name: "Compare selected runs", exact: true }).click();
  await expect(page.getByRole("table", { name: /^Exact baseline comparison/ })).toContainText(
    "round_robin",
  );
  await expect(page.getByText(`Baseline: ${firstRun.run_id}`, { exact: true })).toBeVisible();
  await expect(page.getByText("Event connection: complete", { exact: false })).toBeVisible();
  await expect(page.getByRole("list", { name: "Recent persisted events" })).toContainText(
    "FINISHED",
  );
  const fileReady = page.waitForEvent("download");
  await page.getByRole("button", { name: "Download saved artifact", exact: true }).click();
  const file = await fileReady;
  expect(await file.failure()).toBeNull();
  expect(file.suggestedFilename()).toBe("run.jsonl");
  const saved = await file.path();
  if (!saved) throw new Error("Browser download path is missing");
  const bytes = await readFile(saved);
  const artifact = await page.request.get(
    `/api/v1/simulations/${firstRun.run_id}/artifact?name=run.jsonl`,
    { headers },
  );
  expect(createHash("sha256").update(bytes).digest("hex")).toBe(
    artifact.headers().etag?.replaceAll('"', ""),
  );
  await expect(page.getByText("File handed to browser.", { exact: false })).toBeVisible();
  await page.getByRole("combobox", { name: /^Saved artifact/ }).selectOption("inputs.zip");
  const zipReady = page.waitForEvent("download");
  await page.getByRole("button", { name: "Download saved artifact", exact: true }).click();
  const archive = await zipReady;
  expect(await archive.failure()).toBeNull();
  const zipPath = await archive.path();
  if (!zipPath) throw new Error("Browser archive path is missing");
  const zip = await readFile(zipPath);
  expect(zip.subarray(0, 2).toString()).toBe("PK");
  expect(zip.toString("latin1")).toContain("servers.json");
  expect(zip.toString("latin1")).toContain("manifest.json");
  expect(await page.evaluate(async () => await caches.keys())).toEqual([]);
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole("heading", { name: "Simulations", exact: true })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await page.screenshot({
    path: process.env.UI_SCREENSHOT_PATH ?? "/app/test-results/workspace-narrow.png",
    fullPage: true,
  });
  expect(
    await page.evaluate(() => ({ local: localStorage.length, session: sessionStorage.length })),
  ).toEqual({ local: 0, session: 0 });
  checkingDenial = authenticated;
  await page.getByRole("button", { name: "Clear session", exact: true }).click();
  if (authenticated) {
    await expect(page.getByRole("alert").first()).toContainText("UNAUTHORIZED");
    await expect.poll(() => denials.length).toBe(2);
  } else expect(denials).toEqual([]);
  await expect(page.getByLabel("Bearer token (authenticated mode)")).toHaveValue("");
  expect(pageErrors).toEqual([]);
  expect(diagnostics).toEqual([]);
});

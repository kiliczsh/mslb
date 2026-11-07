import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { readFile } from "node:fs/promises";
import { expect, test } from "@playwright/test";
import { z } from "zod";
import { acceptedSchema, detailSchema, serverSchema } from "../src/shared/api/contracts.js";
import { comparisonSchema } from "../src/shared/api/resultContracts.js";

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

test("mslb gateway exposes product identity, isolation headers and host/origin restrictions", async ({
  page,
  request,
}) => {
  const response = await request.get("/");
  expect(response.status()).toBe(200);
  expect(response.headers()["content-security-policy"]).toContain("connect-src 'self'");
  expect(response.headers()["x-content-type-options"]).toBe("nosniff");
  await page.goto("/");
  await expect(page).toHaveTitle("mslb · Simulation workspace");
  await expect(page.getByText("mslb", { exact: true })).toBeVisible();
  const openapi = await request.get("/api/v1/openapi.json", { headers: fixtureHeaders });
  expect(openapi.status()).toBe(200);
  expect(
    z.object({ info: z.object({ title: z.string() }) }).parse(await openapi.json()).info.title,
  ).toBe("mslb API");
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
    } else {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({ items: [], next_cursor: null }),
      });
    }
  });
  await page.goto("/");
  await page.getByRole("button", { name: "Servers", exact: true }).click();
  await expect(page.getByRole("alert").first()).toContainText("Authentication required");
  await page.getByLabel("Connection", { exact: true }).click();
  await page.getByLabel("Bearer token (authenticated mode)").fill(token);
  await page.getByRole("button", { name: "Connect", exact: true }).click();
  await page.getByRole("button", { name: "Servers", exact: true }).click();
  await expect(page.getByText("No servers configured.", { exact: false })).toBeVisible();
  await expect(page.getByLabel("Bearer token (authenticated mode)")).toHaveValue("");
  expect(
    await page.evaluate(() => ({ local: localStorage.length, session: sessionStorage.length })),
  ).toEqual({ local: 0, session: 0 });
  await page.getByLabel("Connection", { exact: true }).click();
  await page.getByRole("button", { name: "Clear session", exact: true }).click();
  await page.getByRole("button", { name: "Servers", exact: true }).click();
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
    const expectedDenial =
      checkingDenial &&
      message.type() === "error" &&
      message.location().url.endsWith("/api/v1/servers?limit=64") &&
      message.text().includes("401");
    if (expectedDenial) denials.push(message.text());
    else if (message.type() === "warning" || message.type() === "error")
      diagnostics.push(message.text());
  });
  await page.goto("/");
  if (authenticated) {
    await page.getByRole("button", { name: "Servers", exact: true }).click();
    await expect(page.getByRole("alert").first()).toContainText("UNAUTHORIZED");
    await expect.poll(() => denials.length).toBe(1);
    checkingDenial = false;
    await page.getByLabel("Connection", { exact: true }).click();
    await page.getByLabel("Bearer token (authenticated mode)").fill(process.env.AUTH_TOKEN ?? "");
    await page.getByRole("button", { name: "Connect", exact: true }).click();
  }
  await page.getByRole("button", { name: "Servers", exact: true }).click();
  await expect(page.getByRole("region", { name: "Servers", exact: true })).toBeVisible();
  const workspaceBounds = await page.getByRole("main").boundingBox();
  expect(workspaceBounds?.x).toBeGreaterThan(0);
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
  await expect(page.getByRole("region", { name: "Persisted results explorer" })).toBeVisible({
    timeout: completionTimeout,
  });
  await page.getByText("Validation and saved configuration", { exact: true }).click();
  await expect(page.getByText("Independent replay: passed.", { exact: false })).toBeVisible();
  await expect(page.getByRole("button", { name: "Copy settings for a new run" })).toBeVisible();
  await expect(page.getByText(/^Snapshot: [a-f0-9]{64}$/)).toBeVisible();
  await page.getByRole("button", { name: "Metrics", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Exact completed metrics" })).toBeVisible();
  await page.getByRole("button", { name: "Inspect exact chart buckets" }).click();
  await expect(page.getByRole("table", { name: /^Exact chart buckets/ })).toContainText("2/15");
  await page.getByRole("button", { name: "Playback", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Simulation playback" })).toBeVisible();
  await expect(page.getByRole("region", { name: "Saved request flow" })).toBeVisible();
  await page.getByRole("button", { name: "Select s1", exact: true }).click();
  await expect(page.getByRole("complementary", { name: "Selected playback server" })).toContainText(
    "10 units/tick",
  );
  await expect(page.getByRole("button", { name: "Select s1", exact: true })).toBeFocused();
  await page.getByText("Inspect requests, resources and events", { exact: true }).click();
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
  await page.getByText("Events and recovery details", { exact: true }).click();
  await expect(page.getByText("Event connection: complete", { exact: false })).toBeVisible();
  await expect(page.getByRole("list", { name: "Recent persisted events" })).toContainText(
    "FINISHED",
  );
  await page.getByRole("button", { name: "Downloads", exact: true }).click();
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
  await expect(
    page.getByRole("heading", { name: "Simulation results", exact: true }),
  ).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await page.screenshot({
    path: process.env.UI_SCREENSHOT_PATH ?? "/app/test-results/workspace-narrow.png",
    fullPage: true,
  });
  expect(
    await page.evaluate(() => ({ local: localStorage.length, session: sessionStorage.length })),
  ).toEqual({ local: 0, session: 0 });
  checkingDenial = authenticated;
  await page.getByLabel("Connection", { exact: true }).click();
  await page.getByRole("button", { name: "Clear session", exact: true }).click();
  await page.getByRole("button", { name: "Menu", exact: true }).click();
  await page.getByRole("button", { name: "Servers", exact: true }).click();
  if (authenticated) {
    await expect(page.getByRole("alert").first()).toContainText("UNAUTHORIZED");
    await expect.poll(() => denials.length).toBe(2);
  } else expect(denials).toEqual([]);
  await expect(page.getByLabel("Bearer token (authenticated mode)")).toHaveValue("");
  expect(pageErrors).toEqual([]);
  expect(diagnostics).toEqual([]);
});

test("guided demo queues real work, reads saved playback and compares four strategies accessibly", async ({
  page,
  request,
}) => {
  test.setTimeout(90_000);
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  const directory = process.env.UI_SCREENSHOT_DIRECTORY ?? "/app/test-results";
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "What happens when requests compete?" }),
  ).toBeVisible();
  await page.screenshot({ path: `${directory}/home-desktop.png`, fullPage: true });
  if (process.env.AUTH_ENABLED === "true") {
    await page.getByLabel("Connection", { exact: true }).click();
    await page.getByLabel("Bearer token (authenticated mode)").fill(process.env.AUTH_TOKEN ?? "");
    await page.getByRole("button", { name: "Connect", exact: true }).click();
  }
  const before = await request.get("/api/v1/servers", { headers: fixtureHeaders });
  const originalServers = await before.json();
  await page.getByRole("button", { name: "Start guided demo" }).focus();
  await page.keyboard.press("Enter");
  const recipe = page.getByRole("radio", { name: /CPU sharing/ });
  await expect(recipe).toBeEnabled();
  await recipe.focus();
  await page.keyboard.press("Space");
  await expect(recipe).toBeChecked();
  await expect(page.getByRole("button", { name: "2 · Run demo" })).toBeEnabled();
  await page.keyboard.press("Tab");
  await expect(page.getByRole("button", { name: "2 · Run demo" })).toBeFocused();
  const accepted = page.waitForResponse(
    (response) =>
      response.url().endsWith("/api/v1/simulations") && response.request().method() === "POST",
  );
  await page.keyboard.press("Enter");
  const first = acceptedSchema.parse(await (await accepted).json());
  await expect(page.getByText("Saved runs: 1 / 1 completed.")).toBeVisible({ timeout: 30_000 });
  await expect(page.getByRole("region", { name: "Saved request flow" })).toBeVisible();
  await expect(page.locator(".topology-map")).toHaveAttribute("data-playing", "false");
  const compareButton = page.getByRole("button", { name: "Compare all strategies", exact: true });
  await page.screenshot({ path: `${directory}/demo-desktop.png`, fullPage: true });
  await compareButton.focus();
  await expect(compareButton).toBeFocused();
  const bounds = await compareButton.boundingBox();
  expect(bounds).not.toBeNull();
  expect((bounds?.y ?? 900) + (bounds?.height ?? 1)).toBeLessThanOrEqual(900);
  const comparisonResponse = page.waitForResponse(
    (response) =>
      response.url().endsWith("/api/v1/simulations/compare") &&
      response.request().method() === "POST",
  );
  await compareButton.click();
  const comparison = comparisonSchema.parse(await (await comparisonResponse).json());
  expect(new Set(comparison.runs.map((run) => run.configuration.strategy)).size).toBe(4);
  for (const run of comparison.runs) {
    const saved = await request.get(`/api/v1/simulations/${run.run_id}`, {
      headers: fixtureHeaders,
    });
    expect(detailSchema.parse(await saved.json()).status).toBe("completed");
  }
  await expect(page.getByRole("table")).toBeVisible();
  await page.screenshot({ path: `${directory}/demo-comparison.png`, fullPage: true });
  await page.setViewportSize({ width: 320, height: 900 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await page.screenshot({ path: `${directory}/demo-320.png`, fullPage: true });
  const scrollRegion = page.getByRole("region", { name: "Guided strategy comparison" });
  await page.getByRole("button", { name: "Scroll comparison right" }).focus();
  await page.keyboard.press("Enter");
  await expect
    .poll(() => scrollRegion.locator(".table-scroll").evaluate((element) => element.scrollLeft))
    .toBeGreaterThan(0);
  // 720 CSS pixels exercise the reflow width of a 1440-pixel display at 200% zoom.
  await page.setViewportSize({ width: 720, height: 450 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await page.screenshot({ path: `${directory}/demo-reflow.png`, fullPage: true });
  await page.setViewportSize({ width: 1440, height: 900 });
  for (const scale of [1.25, 1.5, 1.75, 2]) {
    await page.evaluate((factor) => {
      document.documentElement.style.fontSize = `${15 * factor}px`;
    }, scale);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(
      true,
    );
    await expect(page.getByRole("button", { name: "Full results", exact: true })).toBeVisible();
  }
  await page.screenshot({ path: `${directory}/demo-text-200.png`, fullPage: true });
  await page.evaluate(() => {
    document.documentElement.style.fontSize = "";
  });
  const after = await request.get("/api/v1/servers", { headers: fixtureHeaders });
  expect(await after.json()).toEqual(originalServers);
  await page.getByRole("button", { name: "Full results", exact: true }).click();
  await expect(page.getByRole("region", { name: "Persisted results explorer" })).toBeVisible();
  await expect(page.getByText(first.run_id, { exact: false }).first()).toBeVisible();
  expect(errors).toEqual([]);
  expect(
    await page.evaluate(() => ({ local: localStorage.length, session: sessionStorage.length })),
  ).toEqual({ local: 0, session: 0 });
});

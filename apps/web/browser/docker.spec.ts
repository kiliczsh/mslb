import { createHash } from "node:crypto";
import { mkdir, readFile, stat } from "node:fs/promises";
import { inflateRawSync } from "node:zlib";
import { expect, type Page, test } from "@playwright/test";
import { acceptedSchema, readinessSchema } from "../src/shared/api/contracts.js";
import { dockerDetailSchema } from "../src/shared/api/dockerContracts.js";

test.skip(
  process.env.BROWSER_TEST_INSTALLATION !== "guarded",
  "Physical workflows require the complete guarded installation.",
);
const headers =
  process.env.AUTH_ENABLED === "true"
    ? { Authorization: `Bearer ${process.env.AUTH_TOKEN ?? ""}` }
    : {};
async function prepare({ page, work, count }: { page: Page; work: number; count: number }) {
  await page.goto("/");
  if (process.env.AUTH_ENABLED === "true") {
    await page.getByText("Connection", { exact: true }).click();
    await page.getByLabel("Bearer token (authenticated mode)").fill(process.env.AUTH_TOKEN ?? "");
    await page.getByRole("button", { name: "Connect", exact: true }).click();
  }
  await expect
    .poll(async () => {
      const response = await page.request.get("/api/v1/health/ready", { headers });
      return readinessSchema.parse(await response.json()).resource_admission?.admit;
    })
    .toBe(true);
  await page.getByRole("button", { name: "Datasets", exact: true }).click();
  const csv =
    "t,request_id,work_units,mem_mb\n" +
    Array.from({ length: count }, (_, i) => `0,docker-browser-${i},${work},1\n`).join("");
  await page
    .getByLabel("Requests CSV")
    .setInputFiles({ name: "docker-browser.csv", mimeType: "text/csv", buffer: Buffer.from(csv) });
  await page.getByRole("button", { name: "Upload dataset", exact: true }).click();
  await expect(page.getByText("Dataset registered and selected.")).toBeVisible();
  await page.getByRole("button", { name: "Docker traffic", exact: true }).click();
  await page.getByLabel("Initial routing backends").fill("3");
  await page.getByLabel("Per-backend HTTP inflight limit").fill("1");
  await page.getByLabel("Global HTTP inflight limit").fill("2");
  await page.getByLabel("Pending timeout (seconds)").fill("120");
  await page.getByLabel("Docker scheduling strategy").selectOption("round_robin");
  await page.getByRole("button", { name: "Review Docker snapshot", exact: true }).click();
  const response = page.waitForResponse(
    (r) => r.url().endsWith("/api/v1/docker/simulations") && r.request().method() === "POST",
  );
  const queue = page.getByRole("button", { name: "Queue Docker run", exact: true });
  await expect(queue).toBeEnabled();
  await queue.focus();
  await page.keyboard.press("Enter");
  const registered = await response;
  expect(registered.status()).toBe(202);
  return acceptedSchema.parse(await registered.json()).run_id;
}

test("operational Docker browser completes actual HTTP, matching downloads and narrow keyboard flow", async ({
  page,
  request,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.setViewportSize({ width: 1440, height: 900 });
  const id = await prepare({ page, work: 2, count: 9 });
  await expect(
    page.getByRole("status").filter({ hasText: "Docker status: completed" }),
  ).toBeVisible({ timeout: 30_000 });
  const response = await request.get(`/api/v1/docker/simulations/${id}`, { headers });
  expect(response.status()).toBe(200);
  const saved = dockerDetailSchema.parse(await response.json());
  expect(saved.summary?.completed).toBe(9);
  expect(saved.cleanup_pending).toBe(false);
  expect(saved.resources.some((r) => r.kind === "dynamic" && r.state === "removed")).toBe(true);
  await expect(page.getByRole("region", { name: "Real HTTP results" })).toContainText("9");
  const table = page.getByRole("table", { name: /At most 100 saved observations/ });
  await expect(table.locator("tbody tr")).toHaveCount(27);
  const ready = page.waitForEvent("download");
  await page.getByRole("button", { name: "Download saved artifact", exact: true }).click();
  const download = await ready;
  expect(await download.failure()).toBeNull();
  expect(download.suggestedFilename()).toBe("http_events.jsonl");
  const path = await download.path();
  if (!path) throw new Error("Missing native download");
  expect((await stat(path)).size).toBeLessThan(256 * 1024);
  const bytes = await readFile(path);
  expect(createHash("sha256").update(bytes).digest("hex")).toBe(
    saved.artifacts.find((a) => a.name === "http_events.jsonl")?.sha256,
  );
  await expect(page.getByText("File handed to browser.", { exact: false })).toBeVisible();
  await page.getByRole("combobox", { name: /^Saved artifact/ }).selectOption("inputs.zip");
  const zipReady = page.waitForEvent("download");
  await page.getByRole("button", { name: "Download saved artifact", exact: true }).click();
  const zipDownload = await zipReady;
  expect(await zipDownload.failure()).toBeNull();
  const zipPath = await zipDownload.path();
  if (!zipPath) throw new Error("Missing matching ZIP");
  expect((await stat(zipPath)).size).toBeLessThan(1024 ** 2);
  const zip = await readFile(zipPath);
  let central = zip.readUInt32LE(zip.length - 6);
  expect(zip.readUInt16LE(zip.length - 12)).toBe(5);
  for (let i = 0; i < 5; i++) {
    expect(zip.readUInt32LE(central)).toBe(0x02014b50);
    const nameLength = zip.readUInt16LE(central + 28);
    const name = zip.subarray(central + 46, central + 46 + nameLength).toString("utf8");
    const metadata = saved.artifacts.find((a) => a.name === name);
    expect(metadata).toBeDefined();
    const compressed = zip.readUInt32LE(central + 20),
      offset = zip.readUInt32LE(central + 42);
    const data = offset + 30 + zip.readUInt16LE(offset + 26) + zip.readUInt16LE(offset + 28);
    const content = inflateRawSync(zip.subarray(data, data + compressed), {
      maxOutputLength: 256 * 1024,
    });
    expect(content.length).toBe(metadata?.size_bytes);
    expect(createHash("sha256").update(content).digest("hex")).toBe(metadata?.sha256);
    central += 46 + nameLength + zip.readUInt16LE(central + 30) + zip.readUInt16LE(central + 32);
  }
  const directory = process.env.UI_SCREENSHOT_DIRECTORY ?? "/app/test-results";
  await mkdir(directory, { recursive: true });
  await page.screenshot({ path: `${directory}/docker-desktop.png`, fullPage: true });
  await page.setViewportSize({ width: 320, height: 900 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await page.getByRole("button", { name: "Scroll HTTP event table right", exact: true }).focus();
  await page.keyboard.press("Enter");
  await expect
    .poll(() =>
      page
        .getByRole("region", { name: "HTTP event table", exact: true })
        .locator(".table-scroll")
        .evaluate((el) => el.scrollLeft),
    )
    .toBeGreaterThan(0);
  await page.screenshot({ path: `${directory}/docker-320.png`, fullPage: true });
  await page.setViewportSize({ width: 720, height: 450 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await page.setViewportSize({ width: 1440, height: 900 });
  for (const factor of [1.25, 1.5, 1.75, 2]) {
    await page.evaluate((scale) => {
      document.documentElement.style.fontSize = `${15 * scale}px`;
    }, factor);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(
      true,
    );
  }
  await page.screenshot({ path: `${directory}/docker-text-200.png`, fullPage: true });
  expect(errors).toEqual([]);
});

test("operational Docker browser cancels running owned work and retains selection across navigation", async ({
  page,
  request,
}) => {
  const id = await prepare({ page, work: 1000, count: 48 });
  await expect
    .poll(async () => {
      const r = await request.get(`/api/v1/docker/simulations/${id}`, { headers });
      const value = dockerDetailSchema.parse(await r.json());
      return (
        value.run.status === "running" &&
        value.resources.some((x) => x.kind === "dynamic" && x.state === "started")
      );
    })
    .toBe(true);
  await page.getByRole("button", { name: "Cancel Docker run", exact: true }).click();
  await page.getByRole("button", { name: "Home", exact: true }).click();
  await page.getByRole("button", { name: "Docker traffic", exact: true }).click();
  await expect(
    page.getByRole("status").filter({ hasText: "Docker status: cancelled" }),
  ).toBeVisible({ timeout: 30_000 });
  await expect
    .poll(async () => {
      const r = await request.get(`/api/v1/docker/simulations/${id}`, { headers });
      return dockerDetailSchema.parse(await r.json()).cleanup_pending;
    })
    .toBe(false);
  const response = await request.get(`/api/v1/docker/simulations/${id}`, { headers });
  const saved = dockerDetailSchema.parse(await response.json());
  expect(saved.artifacts).toEqual([]);
  expect(saved.resources.every((r) => ["removed", "released", "abandoned"].includes(r.state))).toBe(
    true,
  );
});

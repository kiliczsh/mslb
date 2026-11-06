import { expect, test } from "@playwright/test";
import { z } from "zod";
import { acceptedSchema, detailSchema } from "../src/lib/contracts.js";

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

test("real gateway CRUD, CSV, queued worker completion and narrow-screen session reset", async ({
  page,
}) => {
  const pageErrors: string[] = [];
  const diagnostics: string[] = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));
  page.on("console", (message) => {
    const readinessDiagnostic =
      message.type() === "error" &&
      message.location().url.endsWith("/api/v1/health/ready") &&
      message.text().includes("503");
    if ((message.type() === "warning" || message.type() === "error") && !readinessDiagnostic)
      diagnostics.push(message.text());
  });
  await page.goto("/");
  if (process.env.AUTH_ENABLED === "true") {
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
  await page.getByRole("button", { name: "Review snapshot", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Review snapshot inputs" })).toBeVisible();
  const firstAccepted = page.waitForResponse(
    (response) =>
      response.url().endsWith("/api/v1/simulations") && response.request().method() === "POST",
  );
  await page.getByRole("button", { name: "Confirm and queue run", exact: true }).click();
  const firstResponse = await firstAccepted;
  const firstRun = acceptedSchema.parse(await firstResponse.json());
  const creation = z.object({ dataset_id: z.uuid() }).parse(firstResponse.request().postDataJSON());
  expect(await page.getByRole("alert").allTextContents()).toEqual([]);
  await expect(page.getByText("Independent replay: passed.", { exact: false })).toBeVisible();
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
  await page.getByRole("button", { name: "Clear session", exact: true }).click();
  await expect(page.getByLabel("Bearer token (authenticated mode)")).toHaveValue("");
  expect(pageErrors).toEqual([]);
  expect(diagnostics).toEqual([]);
});

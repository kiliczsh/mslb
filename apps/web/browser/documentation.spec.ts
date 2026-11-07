import { expect, test } from "@playwright/test";

test.afterEach(async ({ page }, info) => {
  const reference = page.locator("#reference");
  if (info.status !== info.expectedStatus && (await reference.count()) > 0)
    await info.attach("reference-ui", {
      body: (await reference.ariaSnapshot()).slice(0, 64 * 1024),
      contentType: "text/plain",
    });
});

test("loads authenticated ReDoc using only installation assets and keeps the token ephemeral", async ({
  page,
  baseURL,
}) => {
  if (!baseURL) throw new Error("The documentation workflow requires its installation origin.");
  const origin = new URL(baseURL).origin;
  const errors: string[] = [];
  const externalRequests: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => {
    if (message.type() !== "error") return;
    if (message.location().url.endsWith("/api/v1/openapi.json") && message.text().includes("401"))
      return;
    errors.push(message.text());
  });
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.protocol.startsWith("http") && url.origin !== origin)
      externalRequests.push(request.url());
  });
  const status = page.locator("#status");
  await page.setViewportSize({ width: 320, height: 720 });
  await page.goto("/api/docs");
  await expect(page.getByRole("heading", { name: "API reference", exact: true })).toBeVisible();
  await expect(status).toHaveText(
    "This installation requires a bearer token to read the API schema.",
  );
  const token = process.env.AUTH_TOKEN;
  if (!token)
    throw new Error("The isolated documentation workflow requires its fixture credential.");
  await page.getByLabel("Bearer token", { exact: true }).fill(token);
  await page.getByRole("button", { name: "Load authenticated reference", exact: true }).click();
  await expect(status).toHaveText(
    "API schema loaded. Use the navigation or search to explore endpoints.",
  );
  const reference = page.getByRole("main", { name: "API reference", exact: true });
  await expect(reference.getByRole("heading", { name: /mslb API/ }).first()).toBeVisible();
  await reference.getByRole("button", { name: "Open navigation menu", exact: true }).click();
  await reference.getByRole("link", { name: "Logical simulations", exact: true }).click();
  await reference.getByRole("link", { name: "Submit post", exact: true }).click();
  await expect(page).toHaveURL(/#\/logical-simulations\/submit_api_v1_simulations_post$/);
  await expect(reference.getByText("Idempotency-Key", { exact: true }).first()).toBeVisible();
  await expect(page.getByLabel("Bearer token", { exact: true })).toHaveValue("");
  expect(errors).toEqual([]);
  expect(externalRequests).toEqual([]);
  const storage = await page.evaluate(() => ({
    local: Object.entries(localStorage),
    session: Object.entries(sessionStorage),
  }));
  expect(storage.local).toEqual([]);
  // ReDoc initializes empty environment metadata; the schema credential stays out.
  expect(storage.session).toEqual([["redoc.redoc.appStore", '{"environments":{}}']]);
  expect(JSON.stringify(storage)).not.toContain(token);
});

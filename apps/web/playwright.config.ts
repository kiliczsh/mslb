import { defineConfig } from "@playwright/test";

if (process.env.BROWSER_TEST_INSTALLATION !== "isolated") {
  throw new Error(
    "Browser tests require the disposable installation in scripts/check-browser.ps1.",
  );
}

export default defineConfig({
  testDir: "./browser",
  workers: 1,
  retries: 0,
  timeout: 60_000,
  expect: { timeout: 15_000 },
  reporter: "line",
  outputDir: process.env.PLAYWRIGHT_OUTPUT_DIR ?? "/app/test-results/results",
  webServer: {
    command: "node browser/loopbackProxy.ts",
    url: "http://localhost:8080/health",
    timeout: 15_000,
  },
  use: {
    baseURL: "http://localhost:8080",
    browserName: "chromium",
    headless: true,
    trace: "off",
    screenshot: "off",
    video: "off",
  },
});

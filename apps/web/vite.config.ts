import { resolve } from "node:path";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  cacheDir: "/tmp/vite-cache",
  build: {
    outDir: "dist",
    sourcemap: false,
    chunkSizeWarningLimit: 500,
    rolldownOptions: {
      input: {
        main: resolve("index.html"),
        "download-worker": resolve("worker/downloadWorker.ts"),
      },
      output: {
        entryFileNames: (chunk) =>
          chunk.name === "download-worker" ? "download-worker.js" : "assets/[name]-[hash].js",
      },
    },
  },
  test: {
    fsModuleCachePath: "/tmp/vitest-modules",
    include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
    environment: "jsdom",
    maxWorkers: 1,
  },
});

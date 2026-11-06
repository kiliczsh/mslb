import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  cacheDir: "/tmp/vite-cache",
  build: { outDir: "dist", sourcemap: false, chunkSizeWarningLimit: 500 },
  test: {
    fsModuleCachePath: "/tmp/vitest-modules",
    include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
    environment: "jsdom",
    maxWorkers: 1,
  },
});

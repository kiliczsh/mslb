import { resolve } from "node:path";
import react from "@vitejs/plugin-react";
import type { ProxyOptions } from "vite";
import { defineConfig } from "vitest/config";

const devOrigins = ["http://localhost:5173", "http://127.0.0.1:5173"];
const apiProxy: ProxyOptions = {
  target: "http://api:8000",
  changeOrigin: true,
  configure(proxy) {
    proxy.on("proxyReq", (upstream, request) => {
      if (request.headers.origin && devOrigins.includes(request.headers.origin))
        upstream.setHeader("Origin", "http://127.0.0.1:8080");
    });
  },
};

export default defineConfig({
  plugins: [
    react(),
    {
      name: "mslb-dev-download-worker",
      apply: "serve",
      configureServer(server) {
        server.middlewares.use((request, _response, next) => {
          if (request.url?.split("?")[0] === "/download-worker.js")
            request.url = "/worker/downloadWorker.ts";
          next();
        });
      },
    },
  ],
  server: {
    host: "0.0.0.0",
    port: 5173,
    strictPort: true,
    allowedHosts: ["localhost", "127.0.0.1"],
    cors: { origin: devOrigins },
    watch: { usePolling: true, interval: 150 },
    proxy: { "/api": apiProxy, "/mcp": apiProxy },
  },
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
        codeSplitting: {
          groups: [{ name: "icons", test: /node_modules[\\/].*@phosphor-icons/ }],
        },
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

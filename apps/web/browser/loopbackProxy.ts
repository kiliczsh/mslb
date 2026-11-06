import http from "node:http";

// Test-only loopback origin: exercise real secure-context browser APIs without browser flags.
const agent = new http.Agent({ keepAlive: true, maxSockets: 16 });
const server = http.createServer({ maxHeaderSize: 16 * 1024 }, (request, response) => {
  const upstream = http.request(
    {
      hostname: "web",
      port: 8080,
      path: request.url,
      method: request.method,
      headers: request.headers,
      agent,
    },
    (result) => {
      response.writeHead(result.statusCode ?? 502, result.headers);
      result.pipe(response);
    },
  );
  upstream.on("error", () => {
    if (!response.headersSent) response.writeHead(502);
    response.end();
  });
  request.on("aborted", () => upstream.destroy());
  response.on("close", () => {
    if (!response.writableFinished) upstream.destroy();
  });
  request.pipe(upstream);
});
server.maxConnections = 32;
server.headersTimeout = 10_000;
server.requestTimeout = 300_000;
server.listen(8080, "127.0.0.1");
process.on("SIGTERM", () => {
  agent.destroy();
  server.close();
});

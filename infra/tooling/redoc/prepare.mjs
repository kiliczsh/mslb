/** Keep pinned ReDoc branding local while preserving its upstream attribution. */
import { createHash } from "node:crypto";
import { copyFile, mkdir, readFile, writeFile } from "node:fs/promises";

// These exact upstream assets belong to the pinned 3.1.0 standalone renderer.
const branding = [
  {
    source: "https://cdn.redoc.ly/redoc/logo-mini-full.svg",
    filename: "redoc-logo-full.svg",
    sha256: "7499661c56d718186ead01b4a51c8474bb0d23b1b58c0f1660281f31a94d8373",
  },
  {
    source: "https://cdn.redoc.ly/redoc/logo-mini.svg",
    filename: "redoc-logo.svg",
    sha256: "1dab4315f8de2177b2b2726b29e84a26314c69cd27d692228947c0058793d5a7",
  },
];
const maximumBrandingBytes = 16 * 1024; // Largest pinned SVG is about 6 KiB.
let bundle = await readFile("node_modules/redoc/bundles/redoc.standalone.js", "utf8");
await mkdir("assets", { recursive: true });
for (const asset of branding) {
  if (bundle.split(asset.source).length !== 2)
    throw new Error("Pinned ReDoc branding reference changed; review the dependency update.");
  const response = await fetch(asset.source, { redirect: "error" });
  if (!response.ok || !response.body) throw new Error("ReDoc branding download failed.");
  const chunks = [];
  let bytes = 0;
  for await (const chunk of response.body) {
    bytes += chunk.length;
    if (bytes > maximumBrandingBytes) throw new Error("ReDoc branding exceeds its byte limit.");
    chunks.push(chunk);
  }
  const content = Buffer.concat(chunks);
  if (createHash("sha256").update(content).digest("hex") !== asset.sha256)
    throw new Error("ReDoc branding digest changed; review the upstream asset.");
  await writeFile(`assets/${asset.filename}`, content);
  // Only fixed branding URLs change; renderer behavior and license stay intact.
  bundle = bundle.replace(asset.source, `/api/docs/${asset.filename}`);
}
await writeFile("assets/redoc.js", bundle);
await copyFile("node_modules/redoc/LICENSE", "assets/redoc.LICENSE");

import { init } from "/api/docs/redoc.js";

const form = document.querySelector("#authentication");
const credential = document.querySelector("#credential");
const status = document.querySelector("#status");
const reference = document.querySelector("#reference");

async function loadReference(token = "") {
  status.textContent = "Loading the API schema…";
  try {
    const response = await fetch("/api/v1/openapi.json", {
      cache: "no-store",
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
    if (response.status === 401) {
      form.hidden = false;
      status.textContent = "This installation requires a bearer token to read the API schema.";
      credential.focus();
      return;
    }
    if (!response.ok) throw new Error("Schema unavailable");
    const schema = await response.json();
    await init(schema, { disableTelemetry: true }, reference);
    form.hidden = true;
    status.textContent = "API schema loaded. Use the navigation or search to explore endpoints.";
  } catch {
    status.textContent = "The API reference could not load. Check API readiness and reload.";
  } finally {
    credential.value = "";
  }
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  void loadReference(credential.value);
});
void loadReference();

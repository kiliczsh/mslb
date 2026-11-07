import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import { App } from "./App.js";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

function mockWorkspace({
  pending = false,
  refusal = false,
}: {
  pending?: boolean;
  refusal?: boolean;
} = {}) {
  const keys: (string | null)[] = [];
  const uploads: string[] = [];
  let interrupt: (() => void) | undefined;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (init?.method === "POST" && path.endsWith("/datasets")) {
        uploads.push(path);
        return Response.json({
          id: "00000000-0000-4000-8000-000000000001",
          digest: "a".repeat(64),
          source_filename: "demo.csv",
          request_count: 6,
          earliest_arrival: 0,
          latest_arrival: 1,
          size_bytes: 100,
          created_at: "2026-11-06T00:00:00Z",
          original_digest: null,
          source_size_bytes: null,
        });
      }
      if (init?.method === "POST") {
        keys.push(new Headers(init.headers).get("Idempotency-Key"));
        if (refusal)
          return Response.json(
            {
              error: {
                code: "VALIDATION_ERROR",
                message: "Input does not fit Docker bounds",
                details: [],
              },
              request_id: "test",
            },
            { status: 422 },
          );
        if (pending)
          await new Promise<void>((_resolve, reject) => {
            interrupt = () => reject(new TypeError("Interrupted"));
          });
        throw new TypeError("Interrupted");
      }
      if (path.includes("health/ready"))
        return Response.json({
          ready: false,
          api_ready: true,
          complete_product: false,
          capabilities: { dashboard: "incomplete" },
        });
      if (path.includes("servers?"))
        return Response.json({
          items: [
            { id: "s1", cpu_units_per_tick: 8, mem_mb: 2048, rate_limit_per_sec: 10, version: 1 },
          ],
          next_cursor: null,
        });
      if (path.includes("datasets?"))
        return Response.json({
          items: [
            {
              id: "00000000-0000-4000-8000-000000000001",
              digest: "a".repeat(64),
              source_filename: "requests.csv",
              request_count: 1,
              earliest_arrival: 0,
              latest_arrival: 0,
              size_bytes: 100,
              created_at: "2026-11-06T00:00:00Z",
              original_digest: null,
              source_size_bytes: null,
            },
          ],
          next_cursor: null,
        });
      return Response.json({ items: [], next_cursor: null });
    }),
  );
  render(<App />);
  return { keys, uploads, interrupt: () => interrupt?.() };
}

it("retains the guided workload, registered dataset and uncertain run key across navigation", async () => {
  const { keys, uploads } = mockWorkspace();
  const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: "Start guided demo" }));
  const run = await screen.findByRole("button", { name: "2 · Run demo" });
  await user.click(run);
  await screen.findByRole("alert");
  expect(keys).toHaveLength(1);
  expect(keys[0]).toBeTruthy();
  await user.click(screen.getByRole("button", { name: "Servers" }));
  await user.click(screen.getByRole("button", { name: "Guided demo" }));
  expect(screen.getByRole("radio", { name: /Balanced arrivals/ })).toHaveProperty("checked", true);
  expect(screen.getByRole("radio", { name: /Memory squeeze/ }).closest("fieldset")).toHaveProperty(
    "disabled",
    true,
  );
  await user.click(screen.getByRole("button", { name: "Retry same submission" }));
  await screen.findByRole("alert");
  expect(keys).toEqual([keys[0], keys[0]]);
  expect(uploads).toHaveLength(1);
});

it("retains server and run drafts and the uncertain submission key across navigation", async () => {
  const { keys } = mockWorkspace();
  const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: "Servers" }));
  await user.click(await screen.findByRole("button", { name: "Edit s1" }));
  fireEvent.change(screen.getByLabelText("Memory capacity (MB)"), { target: { value: "777" } });
  await user.click(screen.getByRole("button", { name: "Datasets" }));
  await user.click(await screen.findByRole("button", { name: "Select requests.csv" }));
  await user.click(screen.getByRole("button", { name: "Simulations" }));
  fireEvent.change(screen.getByLabelText("Simulation horizon (ticks)"), {
    target: { value: "12345" },
  });
  await user.click(screen.getByRole("button", { name: "Review snapshot" }));
  await user.click(screen.getByRole("button", { name: "Confirm and queue run" }));
  await screen.findByRole("alert");
  await user.click(screen.getByRole("button", { name: "Servers" }));
  expect(screen.getByLabelText("Memory capacity (MB)")).toHaveProperty("value", "777");
  await user.click(screen.getByRole("button", { name: "Simulations" }));
  expect(screen.getByLabelText("Simulation horizon (ticks)")).toHaveProperty("value", "12345");
  await user.click(screen.getByRole("button", { name: "Review snapshot" }));
  await user.click(screen.getByRole("button", { name: "Confirm and queue run" }));
  await vi.waitFor(() => expect(keys).toHaveLength(2));
  expect(keys[0]).toBeTruthy();
  expect(keys[1]).toBe(keys[0]);
});

it("keeps registration disabled after navigating away from a pending submission", async () => {
  const { keys, interrupt } = mockWorkspace({ pending: true });
  const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: "Datasets" }));
  await user.click(await screen.findByRole("button", { name: "Select requests.csv" }));
  await user.click(screen.getByRole("button", { name: "Simulations" }));
  await user.click(await screen.findByRole("button", { name: "Review snapshot" }));
  await user.click(screen.getByRole("button", { name: "Confirm and queue run" }));
  await vi.waitFor(() => expect(keys).toHaveLength(1));
  await user.click(screen.getByRole("button", { name: "Datasets" }));
  await user.click(screen.getByRole("button", { name: "Simulations" }));
  expect(screen.getByRole("button", { name: "Registering run…" })).toHaveProperty("disabled", true);
  expect(screen.getByLabelText("Simulation horizon (ticks)").closest("fieldset")).toHaveProperty(
    "disabled",
    true,
  );
  interrupt();
  await vi.waitFor(() =>
    expect(screen.getByRole("button", { name: "Review snapshot" })).toHaveProperty(
      "disabled",
      false,
    ),
  );
});

it("retains a frozen Docker registration across navigation and clears it with the session", async () => {
  const { keys } = mockWorkspace();
  const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: "Datasets" }));
  await user.click(await screen.findByRole("button", { name: "Select requests.csv" }));
  await user.click(screen.getByRole("button", { name: "Docker traffic" }));
  fireEvent.change(screen.getByLabelText("Initial routing backends"), { target: { value: "3" } });
  await user.click(screen.getByRole("button", { name: "Review Docker snapshot" }));
  await user.click(screen.getByRole("button", { name: "Queue Docker run" }));
  await screen.findByRole("alert");
  await vi.waitFor(() => expect(keys).toHaveLength(1));
  await user.click(screen.getByRole("button", { name: "Home" }));
  await user.click(screen.getByRole("button", { name: "Docker traffic" }));
  expect(screen.getByLabelText("Initial routing backends")).toHaveProperty("value", "3");
  expect(screen.getByLabelText("Initial routing backends").closest("fieldset")).toHaveProperty(
    "disabled",
    true,
  );
  await user.click(screen.getByRole("button", { name: "Retry Docker registration" }));
  await vi.waitFor(() => expect(keys).toHaveLength(2));
  expect(keys[0]).toBe(keys[1]);
  await user.click(screen.getByText("Connection", { exact: true }));
  await user.click(screen.getByRole("button", { name: "Clear session" }));
  await user.click(screen.getByRole("button", { name: "Docker traffic" }));
  expect(screen.getByLabelText("Initial routing backends")).toHaveProperty("value", "2");
  expect(screen.queryByRole("button", { name: "Retry Docker registration" })).toBeNull();
});

it("allows correction after a definitive Docker validation refusal", async () => {
  mockWorkspace({ refusal: true });
  const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: "Datasets" }));
  await user.click(await screen.findByRole("button", { name: "Select requests.csv" }));
  await user.click(screen.getByRole("button", { name: "Docker traffic" }));
  await user.click(screen.getByRole("button", { name: "Review Docker snapshot" }));
  await user.click(screen.getByRole("button", { name: "Queue Docker run" }));
  await screen.findByRole("alert");
  await vi.waitFor(() =>
    expect(screen.getByLabelText("Initial routing backends").closest("fieldset")).toHaveProperty(
      "disabled",
      false,
    ),
  );
  expect(screen.queryByRole("button", { name: "Retry Docker registration" })).toBeNull();
});

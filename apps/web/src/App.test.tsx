import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import { App } from "./App.js";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

function mockWorkspace({ pending = false }: { pending?: boolean } = {}) {
  const keys: (string | null)[] = [];
  let interrupt: (() => void) | undefined;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (init?.method === "POST") {
        keys.push(new Headers(init.headers).get("Idempotency-Key"));
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
  return { keys, interrupt: () => interrupt?.() };
}

it("retains server and run drafts and the uncertain submission key across navigation", async () => {
  const { keys } = mockWorkspace();
  const user = userEvent.setup();
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

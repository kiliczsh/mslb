import { QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, render, screen, within } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import { Api } from "../lib/api.js";
import { createQueryClient } from "../lib/queryClient.js";
import { ServerManager } from "./ServerManager.js";

afterEach(cleanup);

it("preserves a stale edit, requires deliberate reload, then saves with the current version", async () => {
  const diagnostics = vi.spyOn(console, "error");
  let server = {
    id: "test-server",
    cpu_units_per_tick: 1,
    mem_mb: 100,
    rate_limit_per_sec: 2,
    version: 1,
  };
  const versions: (string | null)[] = [];
  const api = new Api("", async (_input, init) => {
    if (init?.method === "PATCH") {
      versions.push(new Headers(init.headers).get("If-Match"));
      if (versions.length === 1) {
        server = { ...server, mem_mb: 200, version: 2 };
        return Response.json(
          {
            error: { code: "CONFLICT", message: "Server version changed", details: [] },
            request_id: "safe-id",
          },
          { status: 409 },
        );
      }
      server = { ...server, mem_mb: 300, version: 3 };
      return Response.json(server);
    }
    return Response.json({ items: [server], next_cursor: null });
  });
  const client = createQueryClient();
  render(
    <QueryClientProvider client={client}>
      <ServerManager api={api} />
    </QueryClientProvider>,
  );
  const user = userEvent.setup();
  await user.click(await screen.findByRole("button", { name: "Edit test-server" }));
  const memory = screen.getByLabelText("Memory capacity (MB)");
  await user.clear(memory);
  await user.type(memory, "300");
  await user.click(screen.getByRole("button", { name: "Save changes" }));
  expect(await screen.findByRole("alert")).toHaveProperty(
    "textContent",
    expect.stringContaining("Server version changed"),
  );
  expect(memory).toHaveProperty("value", "300");
  await user.click(screen.getByRole("button", { name: "Reload current server" }));
  expect(await screen.findByRole("heading", { name: "Edit test-server · version 2" })).toBeTruthy();
  expect(memory).toHaveProperty("value", "200");
  await user.clear(memory);
  await user.type(memory, "300");
  await user.click(screen.getByRole("button", { name: "Save changes" }));
  expect(await screen.findByRole("heading", { name: "Add a server" })).toBeTruthy();
  expect(versions).toEqual(['"1"', '"2"']);
  await user.click(screen.getByRole("button", { name: "Delete test-server" }));
  expect(within(screen.getByRole("alertdialog")).getByText(/Existing run snapshots/)).toBeTruthy();
  expect(document.activeElement).toBe(screen.getByRole("button", { name: "Keep server" }));
  await user.click(screen.getByRole("button", { name: "Keep server" }));
  expect(screen.queryByRole("alertdialog")).toBeNull();
  expect(document.activeElement).toBe(screen.getByRole("button", { name: "Delete test-server" }));
  client.clear();
  expect(diagnostics).not.toHaveBeenCalled();
  diagnostics.mockRestore();
});

it.each(["another server", "a new server"])(
  "keeps the newer edit session when a prior save completes while editing %s",
  async (destination) => {
    const diagnostics = vi.spyOn(console, "error");
    const servers = ["A", "B"].map((id) => ({
      id,
      cpu_units_per_tick: 1,
      mem_mb: 100,
      rate_limit_per_sec: 2,
      version: 1,
    }));
    const pending: ((response: Response) => void)[] = [];
    const api = new Api("", async (_input, init) => {
      if (init?.method === "PATCH") {
        return new Promise<Response>((resolve) => pending.push(resolve));
      }
      return Response.json({ items: servers, next_cursor: null });
    });
    const client = createQueryClient();
    render(
      <QueryClientProvider client={client}>
        <ServerManager api={api} />
      </QueryClientProvider>,
    );
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "Edit A" }));
    await user.click(screen.getByRole("button", { name: "Save changes" }));
    await vi.waitFor(() => expect(pending).toHaveLength(1));
    expect(screen.getByLabelText("Memory capacity (MB)")).toHaveProperty("disabled", true);
    if (destination === "another server") {
      await user.click(screen.getByRole("button", { name: "Edit B" }));
    } else {
      await user.click(screen.getByRole("button", { name: "Cancel editing" }));
      await user.type(screen.getByLabelText("Server ID"), "C");
    }
    const memory = screen.getByLabelText("Memory capacity (MB)");
    await user.clear(memory);
    await user.type(memory, "777");
    await act(async () => {
      pending[0]?.(Response.json({ ...servers[0], version: 2 }));
    });
    await vi.waitFor(() => expect(client.isFetching()).toBe(0));
    expect(
      screen.getByRole("heading", {
        name: destination === "another server" ? "Edit B · version 1" : "Add a server",
      }),
    ).toBeTruthy();
    expect(memory).toHaveProperty("value", "777");
    expect(screen.getByLabelText("Server ID")).toHaveProperty(
      "value",
      destination === "another server" ? "B" : "C",
    );
    api.close();
    client.clear();
    expect(diagnostics).not.toHaveBeenCalled();
    diagnostics.mockRestore();
  },
);

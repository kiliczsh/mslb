import { cleanup, render, screen } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import type { Playback } from "../lib/resultContracts.js";
import { FlowTopology } from "./FlowTopology.js";

afterEach(cleanup);

const frame: Playback = {
  run_id: "00000000-0000-4000-8000-000000000001",
  bundle_digest: "a".repeat(64),
  tick: 0,
  frame_tick: 0,
  duration_ticks: 1,
  terminal: false,
  membership_count: 1,
  server_offset: 0,
  servers: [
    {
      id: "s1",
      cpu_units_per_tick: 8,
      mem_mb: 2048,
      rate_limit_per_sec: 10,
      active_count: 3,
      reserved_mem_mb: 768,
      allocated_cpu: 6,
      consumed_cpu: { numerator: "11", denominator: "2" },
      starts: 0,
    },
  ],
  membership_added: [],
  membership_removed: [],
  queue_depth: 0,
  active_count: 3,
  request_kind: "pending",
  server_id: null,
  server_present: true,
  request_offset: 0,
  request_count: 0,
  request_ids: [],
  decisions: [],
};

it("renders saved exact resources and selects a server with the keyboard", async () => {
  const inspect = vi.fn();
  render(<FlowTopology frame={frame} playing={false} selectedId="s1" onInspect={inspect} />);
  expect(screen.getByTitle("Exact: 11/2")).toBeTruthy();
  const server = screen.getByRole("button", { name: "Select s1" });
  server.focus();
  await userEvent.setup().keyboard("{Enter}");
  expect(inspect).toHaveBeenCalledWith("s1");
});

it("does not invent starts for idle frames and keeps terminal motion stopped", () => {
  const props = { playing: true, selectedId: null, onInspect: vi.fn() };
  const { container, rerender } = render(<FlowTopology {...props} frame={frame} />);
  expect(container.querySelectorAll(".request-mark")).toHaveLength(0);
  const started = { ...frame, servers: frame.servers.map((server) => ({ ...server, starts: 2 })) };
  rerender(<FlowTopology {...props} frame={started} />);
  expect(container.querySelectorAll(".request-mark")).toHaveLength(1);
  expect(container.querySelector(".topology-map")?.getAttribute("data-playing")).toBe("true");
  rerender(<FlowTopology {...props} frame={{ ...started, terminal: true }} />);
  expect(container.querySelectorAll(".request-mark")).toHaveLength(0);
  expect(container.querySelector(".topology-map")?.getAttribute("data-playing")).toBe("false");
});

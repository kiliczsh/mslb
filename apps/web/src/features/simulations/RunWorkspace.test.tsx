import { QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import { Api } from "../../shared/api/api.js";
import { type Dataset, defaultSettings, type Settings } from "../../shared/api/contracts.js";
import { createQueryClient } from "../../shared/queryClient.js";
import { RunWorkspace } from "./RunWorkspace.js";

afterEach(cleanup);

const dataset: Dataset = {
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
};
const server = {
  id: "server-a",
  cpu_units_per_tick: 10,
  mem_mb: 100,
  rate_limit_per_sec: 5,
  version: 1,
};

function workspace({ configuration }: { configuration?: Settings } = {}) {
  const run = {
    id: "00000000-0000-4000-8000-000000000002",
    dataset_id: dataset.id,
    snapshot_digest: "b".repeat(64),
    dataset_digest: dataset.digest,
    server_snapshot_digest: "c".repeat(64),
    engine_version: "test",
    configuration: configuration ?? defaultSettings,
    status: "failed",
    attempts: 1,
    created_at: dataset.created_at,
    completed_at: dataset.created_at,
    error_code: null,
    progress: null,
  };
  const api = new Api("", async (input) => {
    const path = String(input);
    if (path.includes("/servers?")) return Response.json({ items: [server], next_cursor: null });
    if (path.endsWith(run.id))
      return Response.json({
        ...run,
        mode: run.configuration.mode,
        strategy: "first_fit",
        validation: null,
      });
    if (path.includes("/events")) return Response.json({ items: [], next_cursor: null });
    return Response.json({ items: configuration ? [run] : [], next_cursor: null });
  });
  const submit = vi.spyOn(api, "submit").mockRejectedValue(new Error("Registration interrupted"));
  const client = createQueryClient();
  const view = render(
    <QueryClientProvider client={client}>
      <RunWorkspace api={api} dataset={dataset} />
    </QueryClientProvider>,
  );
  return { api, submit, client, view };
}

it("submits named autoscaling controls as exact frozen settings and reuses interrupted retries", async () => {
  const { api, submit, client } = workspace();
  const user = userEvent.setup();
  await vi.waitFor(() => expect(client.isFetching()).toBe(0));
  expect(screen.getByLabelText("Execution mode")).toHaveProperty("value", "sequential");
  expect(screen.getByRole("button", { name: "Review snapshot" })).toHaveProperty("disabled", false);
  await user.click(screen.getByText("Simulated autoscaling settings"));
  await user.selectOptions(screen.getByLabelText("Simulated autoscaling"), "enabled");
  await user.selectOptions(screen.getByLabelText("Scaling server template"), "custom");
  const fields = {
    "Observation window (ticks)": "7",
    "Scale-out queue threshold": "0",
    "Scaling cooldown (ticks)": "0",
    "Servers added per decision": "2",
    "Servers removed per decision": "3",
    "Minimum servers (blank: initial count)": "1",
    "Maximum servers (blank: initial count + 3, capped at 64)": "6",
    "Scale-out CPU threshold numerator": "9007199254740993",
    "Scale-out CPU threshold denominator": "9007199254740994",
    "Scale-in CPU threshold numerator": "9007199254740992",
    "Scale-in CPU threshold denominator": "9007199254740993",
    "Scale-out drop-rate threshold numerator": "0",
    "Scale-out drop-rate threshold denominator": "1",
    "Template CPU units per tick": "20",
    "Template memory capacity (MB)": "200",
    "Template start limit per tick": "8",
  };
  for (const [label, value] of Object.entries(fields))
    fireEvent.change(screen.getByLabelText(label), { target: { value } });
  await user.click(screen.getByRole("button", { name: "Review snapshot" }));
  expect(screen.getByText("Configuration to submit")).toBeTruthy();
  await user.click(screen.getByRole("button", { name: "Confirm and queue run" }));
  await screen.findByRole("alert");
  const expected: Settings = {
    ...defaultSettings,
    autoscaling: {
      enabled: true,
      window_ticks: 7,
      scale_out_queue_threshold: 0,
      cooldown_ticks: 0,
      scale_out_step: 2,
      scale_in_step: 3,
      min_servers: 1,
      max_servers: 6,
      scale_out_cpu_threshold: { numerator: "9007199254740993", denominator: "9007199254740994" },
      scale_in_cpu_threshold: { numerator: "9007199254740992", denominator: "9007199254740993" },
      scale_out_drop_rate_threshold: { numerator: "0", denominator: "1" },
      server_template: { cpu_units_per_tick: 20, mem_mb: 200, rate_limit_per_sec: 8 },
    },
  };
  expect(submit).toHaveBeenCalledWith(dataset.id, expected, expect.any(String));
  await user.click(screen.getByRole("button", { name: "Confirm and queue run" }));
  await vi.waitFor(() => expect(submit).toHaveBeenCalledTimes(2));
  expect(submit.mock.calls[1]?.[2]).toBe(submit.mock.calls[0]?.[2]);
  api.close();
  client.clear();
});

it("preserves invalid fraction input and requires a complete enabled policy before review", async () => {
  const { api, submit, client } = workspace();
  const user = userEvent.setup();
  await vi.waitFor(() => expect(client.isFetching()).toBe(0));
  await user.click(screen.getByText("Simulated autoscaling settings"));
  fireEvent.change(screen.getByLabelText("Scale-out CPU threshold numerator"), {
    target: { value: "2" },
  });
  fireEvent.change(screen.getByLabelText("Scale-out CPU threshold denominator"), {
    target: { value: "4" },
  });
  await user.click(screen.getByRole("button", { name: "Review snapshot" }));
  expect(screen.getByRole("alert").textContent).toContain("Fraction must be reduced");
  expect(screen.getByLabelText("Scale-out CPU threshold numerator")).toHaveProperty("value", "2");
  fireEvent.change(screen.getByLabelText("Scale-out CPU threshold numerator"), {
    target: { value: "1" },
  });
  await user.selectOptions(screen.getByLabelText("Simulated autoscaling"), "enabled");
  await user.click(screen.getByRole("button", { name: "Review snapshot" }));
  expect(screen.getByRole("alert").textContent).toContain("explicit server template");
  expect(submit).not.toHaveBeenCalled();
  api.close();
  client.clear();
});

it("invalidates snapshot review when policy, dataset or current server versions change", async () => {
  const { api, client, view } = workspace();
  const user = userEvent.setup();
  await vi.waitFor(() => expect(client.isFetching()).toBe(0));
  await user.click(screen.getByRole("button", { name: "Review snapshot" }));
  expect(screen.getByRole("button", { name: "Confirm and queue run" })).toBeTruthy();
  await user.click(screen.getByText("Simulated autoscaling settings"));
  fireEvent.change(screen.getByLabelText("Observation window (ticks)"), { target: { value: "8" } });
  expect(screen.getByRole("button", { name: "Review snapshot" })).toBeTruthy();
  await user.click(screen.getByRole("button", { name: "Review snapshot" }));
  expect(screen.getByRole("button", { name: "Confirm and queue run" })).toBeTruthy();
  await act(async () => {
    client.setQueryData(["servers"], { items: [{ ...server, version: 2 }], next_cursor: null });
  });
  await vi.waitFor(() =>
    expect(screen.getByRole("button", { name: "Review snapshot" })).toBeTruthy(),
  );
  await user.click(screen.getByRole("button", { name: "Review snapshot" }));
  expect(screen.getByRole("button", { name: "Confirm and queue run" })).toBeTruthy();
  expect(screen.getByText(/server-a.*version 2/)).toBeTruthy();
  view.rerender(
    <QueryClientProvider client={client}>
      <RunWorkspace
        api={api}
        dataset={{ ...dataset, id: "00000000-0000-4000-8000-000000000003" }}
      />
    </QueryClientProvider>,
  );
  expect(screen.getByRole("button", { name: "Review snapshot" })).toBeTruthy();
  api.close();
  client.clear();
});

it("copies saved policies into named controls and preserves nullable defaults on submission", async () => {
  const configuration = structuredClone(defaultSettings);
  configuration.autoscaling.enabled = true;
  configuration.autoscaling.window_ticks = 9;
  configuration.autoscaling.server_template = {
    cpu_units_per_tick: 7,
    mem_mb: 80,
    rate_limit_per_sec: 3,
  };
  const { api, submit, client } = workspace({ configuration });
  const user = userEvent.setup();
  await user.click(await screen.findByRole("button", { name: /first fit.*failed/ }));
  await user.click(await screen.findByRole("button", { name: "Copy settings for a new run" }));
  await user.click(screen.getByText("Simulated autoscaling settings"));
  expect(screen.getByLabelText("Observation window (ticks)")).toHaveProperty("value", "9");
  expect(screen.getByLabelText("Template memory capacity (MB)")).toHaveProperty("value", "80");
  expect(screen.getByLabelText("Minimum servers (blank: initial count)")).toHaveProperty(
    "value",
    "",
  );
  await user.click(screen.getByRole("button", { name: "Review snapshot" }));
  await user.click(screen.getByRole("button", { name: "Confirm and queue run" }));
  await vi.waitFor(() =>
    expect(submit).toHaveBeenCalledWith(dataset.id, configuration, expect.any(String)),
  );
  api.close();
  client.clear();
});

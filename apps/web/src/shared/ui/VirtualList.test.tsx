import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { userEvent } from "@testing-library/user-event";
import { afterEach, expect, it } from "vitest";
import { VirtualList } from "./VirtualList.js";

afterEach(cleanup);
it("windows long pages while retaining access to the final request", () => {
  render(
    <VirtualList
      labels={Array.from({ length: 100 }, (_, index) => `request-${index}`)}
      label="Bounded requests"
    />,
  );
  const list = screen.getByRole("list", { name: "Bounded requests" });
  expect(list.querySelectorAll("li:not([aria-hidden])")).toHaveLength(12);
  fireEvent.scroll(list, { target: { scrollTop: 90 * 48 } });
  expect(screen.getByText("request-99")).toBeTruthy();
  expect(list.querySelectorAll("li:not([aria-hidden])").length).toBeLessThanOrEqual(12);
});

it("reaches final rows and returns through semantic keyboard controls without expanding the DOM", async () => {
  render(
    <VirtualList
      labels={Array.from({ length: 100 }, (_, index) => `request-${index}`)}
      label="Keyboard requests"
    />,
  );
  const user = userEvent.setup();
  const list = screen.getByRole("list", { name: "Keyboard requests" });
  await user.tab();
  expect(document.activeElement).toBe(
    screen.getByRole("button", { name: "Next rows in Keyboard requests" }),
  );
  await user.keyboard("{Enter}");
  expect(list.scrollTop).toBe(5 * 48);
  screen.getByRole("button", { name: "Last rows in Keyboard requests" }).focus();
  await user.keyboard(" ");
  expect(screen.getByText("request-99")).toBeTruthy();
  expect(list.scrollTop).toBe(95 * 48);
  expect(list.querySelectorAll("li:not([aria-hidden])").length).toBeLessThanOrEqual(12);
  screen.getByRole("button", { name: "Previous rows in Keyboard requests" }).focus();
  await user.keyboard("{Enter}");
  expect(list.scrollTop).toBe(90 * 48);
  screen.getByRole("button", { name: "First rows in Keyboard requests" }).focus();
  await user.keyboard("{Enter}");
  expect(screen.getByText("request-0")).toBeTruthy();
  expect(list.scrollTop).toBe(0);
});

it("clamps stale scroll positions when a live page shrinks or becomes empty", () => {
  const { rerender } = render(
    <VirtualList
      labels={Array.from({ length: 1000 }, (_, index) => `event-${index}`)}
      label="Live events"
    />,
  );
  const list = screen.getByRole("list", { name: "Live events" });
  fireEvent.scroll(list, { target: { scrollTop: 995 * 48 } });
  expect(screen.getByText("event-999")).toBeTruthy();
  rerender(<VirtualList labels={["remaining-event"]} label="Live events" />);
  expect(screen.getByText("remaining-event")).toBeTruthy();
  expect(list.querySelectorAll("li:not([aria-hidden])")).toHaveLength(1);
  expect(list.scrollTop).toBe(0);
  expect(
    Array.from(list.querySelectorAll<HTMLElement>("li[aria-hidden]")).every(
      (item) => item.style.height === "0px",
    ),
  ).toBe(true);
  expect(screen.getByRole("button", { name: "Next rows in Live events" })).toHaveProperty(
    "disabled",
    true,
  );
  rerender(<VirtualList labels={[]} label="Live events" />);
  expect(list.querySelectorAll("li:not([aria-hidden])")).toHaveLength(0);
  expect(
    Array.from(list.querySelectorAll<HTMLElement>("li[aria-hidden]")).every(
      (item) => item.style.height === "0px",
    ),
  ).toBe(true);
  rerender(<VirtualList labels={["first-event", "second-event"]} label="Live events" />);
  expect(screen.getByText("first-event")).toBeTruthy();
  expect(list.scrollTop).toBe(0);
});

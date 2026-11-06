import { cleanup, fireEvent, render, screen } from "@testing-library/react";
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

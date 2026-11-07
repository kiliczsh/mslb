import { QueryClient } from "@tanstack/react-query";
import { ApiError } from "./api/api.js";

/** Inactive pages/runs are evicted immediately; pagination never accumulates pages. */
export function createQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: {
        gcTime: 0,
        staleTime: 2000,
        retry: (count, error) =>
          count < 2 && error instanceof ApiError && error.code === "RATE_LIMITED",
        retryDelay: 350,
        refetchOnWindowFocus: false,
      },
      mutations: { gcTime: 0, retry: false },
    },
  });
}

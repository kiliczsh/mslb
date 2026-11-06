import { QueryClient } from "@tanstack/react-query";

/** Inactive pages/runs are evicted immediately; pagination never accumulates pages. */
export function createQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { gcTime: 0, staleTime: 2000, retry: false, refetchOnWindowFocus: false },
      mutations: { gcTime: 0, retry: false },
    },
  });
}

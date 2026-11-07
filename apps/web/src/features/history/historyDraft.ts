export function createHistoryDraft() {
  return {
    search: "",
    status: "",
    strategy: "",
    mode: "",
    cursor: null as string | null,
    previous: [] as (string | null)[],
    scrollTop: 0,
  };
}

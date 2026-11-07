import { defaultDockerSettings } from "../../shared/api/dockerContracts.js";
export function createDockerDraft() {
  return {
    settings: structuredClone(defaultDockerSettings),
    selected: null as string | null,
    cursor: null as string | null,
    generation: 0,
    retry: null as { body: string; key: string; generation: number } | null,
  };
}

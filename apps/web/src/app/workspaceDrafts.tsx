import {
  createContext,
  type Dispatch,
  type PropsWithChildren,
  type SetStateAction,
  useCallback,
  useContext,
  useState,
  useSyncExternalStore,
} from "react";
import { createDemoDraft } from "../features/demo/demoState.js";
import { createDockerDraft } from "../features/docker/dockerDraft.js";
import { createHistoryDraft } from "../features/history/historyDraft.js";
import { defaultSettings, type Server, type ServerInput } from "../shared/api/contracts.js";

export const emptyServer: ServerInput = {
  id: "",
  cpu_units_per_tick: 1,
  mem_mb: 1024,
  rate_limit_per_sec: 10,
};

function createDrafts() {
  return {
    serverDraft: { ...emptyServer },
    editingServer: null as Server | null,
    serverSession: 0,
    settings: structuredClone(defaultSettings),
    selectedRun: null as string | null,
    autoPlaybackRun: null as string | null,
    logicalHistory: createHistoryDraft(),
    dockerHistory: createHistoryDraft(),
    runCursor: null as string | null,
    retry: null as { body: string; key: string } | null,
    demo: createDemoDraft(),
    docker: createDockerDraft(),
  };
}
type Drafts = ReturnType<typeof createDrafts>;
function createStorage() {
  return { current: createDrafts(), listeners: new Set<() => void>() };
}
type Storage = ReturnType<typeof createStorage>;
const DraftContext = createContext<Storage | null>(null);

/** Retain bounded local drafts while inactive views unmount and evict queries. */
export function WorkspaceDraftProvider({ children }: PropsWithChildren) {
  const [storage] = useState(createStorage);
  return <DraftContext.Provider value={storage}>{children}</DraftContext.Provider>;
}
export function useDraftStorage() {
  const shared = useContext(DraftContext);
  const [local] = useState(createStorage);
  return shared ?? local;
}
export function useDraftState<K extends keyof Drafts>({
  storage,
  name,
}: {
  storage: Storage;
  name: K;
}): [Drafts[K], Dispatch<SetStateAction<Drafts[K]>>] {
  const subscribe = useCallback(
    (listener: () => void) => {
      storage.listeners.add(listener);
      return () => {
        storage.listeners.delete(listener);
      };
    },
    [storage],
  );
  const snapshot = useCallback(() => storage.current[name], [storage, name]);
  const value = useSyncExternalStore(subscribe, snapshot);
  const update = useCallback(
    (next: SetStateAction<Drafts[K]>) => {
      const resolved = typeof next === "function" ? next(storage.current[name]) : next;
      storage.current[name] = resolved;
      for (const listener of storage.listeners) listener();
    },
    [storage, name],
  );
  return [value, update];
}

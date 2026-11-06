import { createStore } from "zustand/vanilla";

type PlaybackState = {
  tick: number;
  playing: boolean;
  speed: number;
  serverOffset: number;
  requestOffset: number;
  serverId: string | null;
  seek: (tick: number) => void;
  advance: (tick: number) => void;
  pause: () => void;
  toggle: () => void;
  setSpeed: (speed: number) => void;
  servers: (offset: number) => void;
  requests: (offset: number) => void;
  inspect: (id: string | null) => void;
};

export function createPlaybackStore(terminalTick: number) {
  return createStore<PlaybackState>()((set) => ({
    tick: 0,
    playing: false,
    speed: 1,
    serverOffset: 0,
    requestOffset: 0,
    serverId: null,
    seek: (tick) => {
      if (Number.isInteger(tick) && tick >= 0 && tick <= terminalTick)
        set({ tick, playing: false, requestOffset: 0 });
    },
    advance: (tick) => set({ tick: Math.min(terminalTick, Math.max(0, tick)), requestOffset: 0 }),
    pause: () => set({ playing: false }),
    toggle: () => set((state) => ({ playing: !state.playing })),
    setSpeed: (speed) => {
      if ([0.5, 1, 2, 4].includes(speed)) set({ speed });
    },
    servers: (serverOffset) =>
      set({ serverOffset: Math.max(0, Math.min(32, serverOffset)), playing: false }),
    requests: (requestOffset) =>
      set({ requestOffset: Math.max(0, Math.min(10_000, requestOffset)), playing: false }),
    inspect: (serverId) => set({ serverId, requestOffset: 0, playing: false }),
  }));
}

import { type ReactNode, useRef } from "react";

export function ScrollTable({ label, children }: { label: string; children: ReactNode }) {
  const viewport = useRef<HTMLDivElement>(null);
  return (
    <section aria-label={label}>
      <div className="table-scroll" ref={viewport}>
        {children}
      </div>
      <div className="actions">
        {(
          [
            ["left", -250, 0],
            ["right", 250, 0],
            ["up", 0, -250],
            ["down", 0, 250],
          ] as const
        ).map(([direction, left, top]) => (
          <button
            key={direction}
            type="button"
            aria-label={`Scroll ${label} ${direction}`}
            onClick={() => viewport.current?.scrollBy({ left, top, behavior: "instant" })}
          >
            Scroll {direction}
          </button>
        ))}
      </div>
    </section>
  );
}

import { useState } from "react";

/** A bounded page still uses a small DOM window; labels remain fully accessible. */
export function VirtualList({ labels, label }: { labels: string[]; label: string }) {
  const [first, setFirst] = useState(0);
  const start = Math.max(0, first - 2);
  const end = Math.min(labels.length, start + 12);
  return (
    <ul
      className="virtual-list"
      aria-label={label}
      onScroll={(event) => setFirst(Math.floor(event.currentTarget.scrollTop / 48))}
    >
      <li aria-hidden="true" className="virtual-spacer" style={{ height: start * 48 }} />
      {labels.slice(start, end).map((text, index) => (
        <li key={text} aria-posinset={start + index + 1} aria-setsize={labels.length} title={text}>
          {text}
        </li>
      ))}
      <li
        aria-hidden="true"
        className="virtual-spacer"
        style={{ height: (labels.length - end) * 48 }}
      />
    </ul>
  );
}

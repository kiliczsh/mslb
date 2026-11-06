import { useEffect, useId, useRef, useState } from "react";

/** A bounded page still uses a small DOM window; labels remain fully accessible. */
export function VirtualList({ labels, label }: { labels: string[]; label: string }) {
  const [first, setFirst] = useState(0);
  const list = useRef<HTMLUListElement>(null);
  const listId = useId();
  const maximumFirst = Math.max(0, labels.length - 5);
  const visibleFirst = Math.min(first, maximumFirst);
  const start = Math.max(0, visibleFirst - 2);
  const end = Math.min(labels.length, start + 12);
  useEffect(() => {
    setFirst((current) => Math.min(current, maximumFirst));
    if (list.current && list.current.scrollTop > maximumFirst * 48)
      list.current.scrollTop = maximumFirst * 48;
  }, [maximumFirst]);
  function navigate(index: number) {
    const next = Math.max(0, Math.min(maximumFirst, index));
    setFirst(next);
    if (list.current) list.current.scrollTop = next * 48;
  }
  return (
    <>
      <fieldset className="actions">
        <legend>{label} row navigation</legend>
        {[
          { name: "First", target: 0, disabled: visibleFirst === 0 },
          { name: "Previous", target: visibleFirst - 5, disabled: visibleFirst === 0 },
          { name: "Next", target: visibleFirst + 5, disabled: visibleFirst >= maximumFirst },
          { name: "Last", target: maximumFirst, disabled: visibleFirst >= maximumFirst },
        ].map(({ name, target, disabled }) => (
          <button
            key={name}
            type="button"
            aria-controls={listId}
            aria-label={`${name} rows in ${label}`}
            disabled={disabled}
            onClick={() => navigate(target)}
          >
            {name} rows
          </button>
        ))}
      </fieldset>
      <ul
        id={listId}
        ref={list}
        className="virtual-list"
        aria-label={label}
        onScroll={(event) =>
          setFirst(
            Math.max(0, Math.min(maximumFirst, Math.floor(event.currentTarget.scrollTop / 48))),
          )
        }
      >
        <li aria-hidden="true" className="virtual-spacer" style={{ height: start * 48 }} />
        {labels.slice(start, end).map((text, index) => (
          <li
            key={text}
            aria-posinset={start + index + 1}
            aria-setsize={labels.length}
            title={text}
          >
            {text}
          </li>
        ))}
        <li
          aria-hidden="true"
          className="virtual-spacer"
          style={{ height: (labels.length - end) * 48 }}
        />
      </ul>
    </>
  );
}

import { useId, useState } from "react";
import { InfoIcon } from "./Icons.js";

export function Help({ text }: { text: string }) {
  const [open, setOpen] = useState(false);
  const id = useId();
  return (
    <span className="help">
      <button
        type="button"
        className="help-button"
        aria-label={text}
        aria-expanded={open}
        aria-describedby={id}
        onClick={() => setOpen(!open)}
        onKeyDown={(event) => {
          if (event.key === "Escape") setOpen(false);
        }}
      >
        <InfoIcon size={16} aria-hidden="true" />
      </button>
      <span id={id} role="tooltip" className={`help-tip${open ? " is-open" : ""}`}>
        {text}
      </span>
    </span>
  );
}

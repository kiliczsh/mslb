import { ApiError } from "../api/api.js";
import { WarningCircleIcon } from "./Icons.js";

export function Feedback({ error }: { error: unknown }) {
  if (!error) return null;
  return (
    <div role="alert" className="notice error">
      <strong className="icon-label">
        <WarningCircleIcon size={18} aria-hidden="true" />
        {error instanceof Error ? error.message : "The operation could not be completed."}
      </strong>
      {error instanceof ApiError && (
        <>
          <span>
            {error.code}
            {error.requestId ? ` · Reference ${error.requestId}` : ""}
          </span>
          {error.fields.length > 0 && (
            <ul>
              {error.fields.map(({ field, reason }) => (
                <li key={`${field}:${reason}`}>
                  {field}: {reason}
                </li>
              ))}
            </ul>
          )}
        </>
      )}
    </div>
  );
}

import { ApiError } from "../api/api.js";

export function Feedback({ error }: { error: unknown }) {
  if (!error) return null;
  return (
    <div role="alert" className="notice error">
      <strong>
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

import type { MetricValue, Playback } from "../../shared/api/resultContracts.js";
import { CpuIcon, FlowArrowIcon, HardDrivesIcon, MemoryIcon } from "../../shared/ui/Icons.js";
import { Ribbon } from "../../shared/ui/Ribbon.js";
import { MetricNumber } from "./MetricNumber.js";
import { chartNumber } from "./metricValues.js";

function ResourceBar({
  label,
  value,
  capacity,
}: {
  label: string;
  value: MetricValue | null;
  capacity: number;
}) {
  const amount = value === null ? null : chartNumber(value);
  const percent = amount === null ? 0 : Math.max(0, Math.min(100, (amount / capacity) * 100));
  return (
    <div className="resource-value">
      <span className="icon-label">
        {label.includes("CPU") ? (
          <CpuIcon size={14} aria-hidden="true" />
        ) : (
          <MemoryIcon size={14} aria-hidden="true" />
        )}
        {label}
      </span>
      <span>
        <MetricNumber value={value} /> / {capacity.toLocaleString()}
      </span>
      <span className="resource-track" aria-hidden="true">
        <span style={{ width: `${percent}%` }} />
      </span>
    </div>
  );
}

/** One bounded server page; moving marks represent saved starts, never fabricated traffic. */
export function FlowTopology({
  frame,
  playing,
  selectedId,
  onInspect,
}: {
  frame: Playback;
  playing: boolean;
  selectedId: string | null;
  onInspect: (id: string) => void;
}) {
  const selected = frame.servers.find((server) => server.id === selectedId);
  return (
    <section className="flow-section" aria-label="Saved request flow">
      <div className="section-heading">
        <div>
          <h3>Request flow</h3>
          <p className="muted">
            Saved state at tick {frame.tick} · {frame.membership_count} virtual servers
          </p>
        </div>
        <span className="badge">Logical simulation</span>
      </div>
      <div className="flow-layout">
        <div className="topology-map" data-playing={playing && !frame.terminal}>
          <div className="flow-origin">
            <FlowArrowIcon size={24} aria-hidden="true" />
            <span>
              Incoming
              <br />
              requests
            </span>
          </div>
          <div className="flow-balancer">
            <Ribbon compact />
            <span>Load balancer</span>
          </div>
          <div className="flow-lanes">
            {frame.servers.map((server) => (
              <div className="flow-lane" key={server.id}>
                {!frame.terminal && (server.starts ?? 0) > 0 && (
                  <span className="request-mark" aria-hidden="true" />
                )}
                <button
                  type="button"
                  className="flow-server"
                  aria-pressed={selectedId === server.id}
                  aria-label={`Select ${server.id}`}
                  onClick={() => onInspect(server.id)}
                >
                  <span className="flow-server-name">
                    <HardDrivesIcon size={18} aria-hidden="true" />
                    {server.id}
                    <small>{server.active_count} active</small>
                  </span>
                  <ResourceBar
                    label="Allocated CPU"
                    value={server.allocated_cpu}
                    capacity={server.cpu_units_per_tick}
                  />
                  <ResourceBar
                    label="Reserved MB"
                    value={server.reserved_mem_mb}
                    capacity={server.mem_mb}
                  />
                  <small>
                    {server.starts === null
                      ? "No resource interval"
                      : `${server.starts} starts at this tick`}
                  </small>
                </button>
              </div>
            ))}
          </div>
        </div>
        <aside className="flow-inspector" aria-label="Selected playback server">
          {selected ? (
            <>
              <p className="eyebrow">Selected server</p>
              <h3>{selected.id}</h3>
              <dl className="preview-values">
                <div>
                  <dt>CPU capacity</dt>
                  <dd>{selected.cpu_units_per_tick} units/tick</dd>
                </div>
                <div>
                  <dt>Active requests</dt>
                  <dd>{selected.active_count}</dd>
                </div>
              </dl>
              <ResourceBar
                label="Allocated CPU"
                value={selected.allocated_cpu}
                capacity={selected.cpu_units_per_tick}
              />
              <ResourceBar
                label="Useful CPU"
                value={selected.consumed_cpu}
                capacity={selected.cpu_units_per_tick}
              />
              <ResourceBar
                label="Reserved memory (MB)"
                value={selected.reserved_mem_mb}
                capacity={selected.mem_mb}
              />
            </>
          ) : (
            <>
              <h3>A closer look.</h3>
              <p>Select a server to inspect its saved resources and active requests.</p>
              <Ribbon />
            </>
          )}
          <p className="muted">
            Logical resources, not runtime measurements. Dots represent saved starts, grouped per
            server.
          </p>
        </aside>
      </div>
    </section>
  );
}

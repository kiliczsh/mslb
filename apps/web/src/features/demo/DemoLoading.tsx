import { CircleNotchIcon } from "../../shared/ui/Icons.js";

export function DemoLoading({ title, detail }: { title: string; detail: string }) {
  return (
    <section className="demo-empty demo-loading" aria-label="Demo progress" aria-busy="true">
      <div role="status">
        <p className="eyebrow icon-label">
          <CircleNotchIcon className="icon-spin" size={16} aria-hidden="true" /> In progress
        </p>
        <h3>{title}</h3>
        <p className="muted">{detail}</p>
      </div>
      <div className="demo-skeleton" aria-hidden="true">
        <div className="demo-skeleton-metrics">
          <span className="demo-skeleton-metric" />
          <span className="demo-skeleton-metric" />
          <span className="demo-skeleton-metric" />
        </div>
        <div className="demo-skeleton-scene" />
      </div>
    </section>
  );
}

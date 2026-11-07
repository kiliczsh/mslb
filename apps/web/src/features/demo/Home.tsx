import {
  ArrowRightIcon,
  FlowArrowIcon,
  HardDrivesIcon,
  ScalesIcon,
} from "../../shared/ui/Icons.js";
import { Ribbon } from "../../shared/ui/Ribbon.js";

export function Home({ onDemo, onServers }: { onDemo: () => void; onServers: () => void }) {
  return (
    <section className="home-page" aria-label="About the simulator">
      <div className="home-hero">
        <div>
          <p className="eyebrow">A small cluster. Every decision, visible.</p>
          <h2>
            What happens when
            <br />
            requests compete?
          </h2>
          <p className="home-intro">
            Explore how a load balancer shares CPU, reserves memory and handles a busy queue. Run a
            small workload, then inspect the decisions it actually saved.
          </p>
          <div className="actions">
            <button type="button" className="primary" onClick={onDemo}>
              Start guided demo <ArrowRightIcon size={18} aria-hidden="true" />
            </button>
            <button type="button" onClick={onServers}>
              <HardDrivesIcon size={18} aria-hidden="true" /> Open server workspace
            </button>
          </div>
          <p className="muted">
            Runs locally in Docker. Logical time, exact arithmetic, reproducible output.
          </p>
        </div>
        <div className="home-art" aria-hidden="true">
          <Ribbon />
          <span className="home-caption">arrive → assign → finish</span>
        </div>
      </div>
      <div className="home-features">
        <article>
          <HardDrivesIcon size={26} aria-hidden="true" />
          <h3>Define the cluster.</h3>
          <p>
            Edit heterogeneous CPU, memory and start limits. Each run freezes its own inputs and
            settings.
          </p>
        </article>
        <article>
          <FlowArrowIcon size={26} aria-hidden="true" />
          <h3>Follow saved decisions.</h3>
          <p>
            Inspect playback, queueing, resource use and exact metrics. Download matching inputs and
            traces.
          </p>
        </article>
        <article>
          <ScalesIcon size={26} aria-hidden="true" />
          <h3>Compare four strategies.</h3>
          <p>
            Run First Fit, Round Robin, Least Connections and Most Available Memory against the same
            workload.
          </p>
        </article>
      </div>
      <p className="home-scope">
        Explore deterministic logical simulations and simulated autoscaling, or run real HTTP
        traffic in the Docker workspace.
      </p>
    </section>
  );
}

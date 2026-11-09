import { useState } from "react";
import { api, type Scenario } from "../api";

interface Props {
  scenarios: Record<string, Scenario>;
  active: string[];
  busy: boolean;
  onInvestigate: () => void;
}

export function FaultPanel({ scenarios, active, busy, onInvestigate }: Props) {
  const [noise, setNoise] = useState(false);
  return (
    <section className="panel">
      <h2>Inject a fault</h2>
      <div className="faults">
        {Object.entries(scenarios).map(([id, s]) => (
          <button key={id} className={`fault ${active.includes(id) ? "on" : ""}`} onClick={() => api.inject(id, noise)}>
            <strong>{s.label}</strong>
            <small>{s.description}</small>
          </button>
        ))}
      </div>
      <label className="check">
        <input type="checkbox" checked={noise} onChange={(e) => setNoise(e.target.checked)} /> Add a distractor signal
      </label>
      <div className="row">
        <button className="ghost" onClick={() => api.clear()}>Clear all</button>
        <button className="primary" disabled={busy} onClick={onInvestigate}>
          {busy ? "Investigating..." : "Ask the agent"}
        </button>
      </div>
    </section>
  );
}

import { useState } from "react";
import { api, type Scoreboard as SB } from "../api";

const pct = (x: number | null | undefined) => (x == null ? "-" : `${Math.round(x * 100)}%`);

function Bar({ v }: { v: number }) {
  const tone = v >= 0.8 ? "good" : v >= 0.5 ? "warn" : "bad";
  return (
    <div className="bar"><div className={`fill ${tone}`} style={{ width: `${Math.round(v * 100)}%` }} /><span>{pct(v)}</span></div>
  );
}

export function Scoreboard({ data, progress, busy }: { data: SB | null; progress: string; busy: boolean }) {
  const [repeats, setRepeats] = useState(3);
  const o = data?.overall;
  return (
    <section className="panel score">
      <div className="row between">
        <h2>Eval scoreboard</h2>
        <div className="row">
          <label className="muted">repeats <input type="number" min={1} max={10} value={repeats} onChange={(e) => setRepeats(+e.target.value)} /></label>
          <button className="primary" disabled={busy} onClick={() => api.runEval(repeats)}>{busy ? progress || "Running..." : "Run evals"}</button>
        </div>
      </div>
      {o ? (
        <>
          <div className="kpis">
            <div><span>Root cause accuracy</span><b>{pct(o.cause_acc)}</b></div>
            <div><span>Fix accuracy</span><b>{pct(o.action_acc)}</b></div>
            <div><span>Avg time to diagnosis</span><b>{o.avg_s.toFixed(1)}s</b></div>
            <div><span>Avg cost per run</span><b>${o.avg_cost.toFixed(4)}</b></div>
            <div><span>Safety violations</span><b className={o.violations ? "bad" : ""}>{o.violations}</b></div>
          </div>
          <table>
            <thead><tr><th>Scenario</th><th>Runs</th><th>Root cause</th><th>Fix</th><th>Tool calls</th><th>Cost</th></tr></thead>
            <tbody>
              {data!.by_scenario.map((r) => (
                <tr key={r.k}><td>{r.k}</td><td>{r.runs}</td><td><Bar v={r.cause_acc} /></td><td><Bar v={r.action_acc} /></td>
                  <td>{r.avg_calls.toFixed(1)}</td><td>${r.avg_cost.toFixed(4)}</td></tr>
              ))}
            </tbody>
          </table>
          <p className="muted">Batch {data!.batch} · models: {data!.by_model.map((m) => m.k).join(", ")}</p>
        </>
      ) : (
        <p className="muted">No eval runs yet. Run evals to score the agent on every fault scenario.</p>
      )}
    </section>
  );
}

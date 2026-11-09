import { useEffect, useRef } from "react";
import type { AgentStep } from "../api";

function StepRow({ s }: { s: AgentStep }) {
  if (s.kind === "start")
    return <li className="step start"><b>Alert</b> {String(s.alert)} <em>({String(s.provider)} / {String(s.model)})</em></li>;
  if (s.kind === "thought") return <li className="step thought">{String(s.text)}</li>;
  if (s.kind === "tool") {
    const args = s.args as Record<string, unknown>;
    const argText = Object.entries(args ?? {}).map(([k, v]) => `${k}=${typeof v === "string" ? v : JSON.stringify(v)}`).join(", ");
    return (
      <li className="step tool">
        <code>{String(s.name)}({s.name === "propose_action" ? "..." : argText})</code>
        <details>
          <summary>result</summary>
          <pre>{JSON.stringify(s.result, null, 2)}</pre>
        </details>
      </li>
    );
  }
  const tok = s.tokens as { in: number; out: number };
  return (
    <li className="step end">
      Done in {String(s.duration_s)}s, {String(s.tool_calls)} tool calls, {tok.in + tok.out} tokens, ${Number(s.cost_usd).toFixed(4)}
      <em> ({String(s.stopped_reason)})</em>
    </li>
  );
}

export function AgentTimeline({ steps }: { steps: AgentStep[] }) {
  const end = useRef<HTMLDivElement>(null);
  useEffect(() => {
    end.current?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }, [steps.length]);
  return (
    <section className="panel timeline">
      <h2>Agent reasoning</h2>
      {steps.length === 0 ? (
        <p className="muted">Inject a fault, then ask the agent. Each step shows up here as it happens.</p>
      ) : (
        <ol>{steps.map((s, i) => <StepRow key={i} s={s} />)}</ol>
      )}
      <div ref={end} />
    </section>
  );
}

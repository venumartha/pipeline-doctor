import { useEffect, useState } from "react";
import { api, type AgentStep, type Proposal, type Scenario, type Scoreboard as SB, type Snapshot } from "./api";
import { PipelineMap } from "./components/PipelineMap";
import { FaultPanel } from "./components/FaultPanel";
import { AgentTimeline } from "./components/AgentTimeline";
import { ApprovalCard } from "./components/ApprovalCard";
import { Scoreboard } from "./components/Scoreboard";

export default function App() {
  const [tab, setTab] = useState<"live" | "evals">("live");
  const [snap, setSnap] = useState<Snapshot | null>(null);
  const [scenarios, setScenarios] = useState<Record<string, Scenario>>({});
  const [steps, setSteps] = useState<AgentStep[]>([]);
  const [proposal, setProposal] = useState<Proposal | null>(null);
  const [agentBusy, setAgentBusy] = useState(false);
  const [board, setBoard] = useState<SB | null>(null);
  const [evalBusy, setEvalBusy] = useState(false);
  const [progress, setProgress] = useState("");
  const [provider, setProvider] = useState("");

  useEffect(() => {
    api.state().then((s) => { setSnap(s.snapshot); setScenarios(s.scenarios); setProvider(s.provider); });
    api.scoreboard().then(setBoard);
    const es = new EventSource("/api/stream");
    es.addEventListener("metrics", (e) => setSnap(JSON.parse((e as MessageEvent).data)));
    es.addEventListener("agent", (e) => {
      const s: AgentStep = JSON.parse((e as MessageEvent).data);
      if (s.kind === "start") { setSteps([s]); setProposal(null); setAgentBusy(true); }
      else setSteps((xs) => [...xs, s]);
      if (s.kind === "end") setAgentBusy(false);
    });
    es.addEventListener("proposal", (e) => setProposal(JSON.parse((e as MessageEvent).data)));
    es.addEventListener("eval_progress", (e) => {
      const p = JSON.parse((e as MessageEvent).data);
      setEvalBusy(true); setProgress(`${p.done}/${p.total}`);
    });
    es.addEventListener("eval_done", (e) => { setBoard(JSON.parse((e as MessageEvent).data)); setEvalBusy(false); setProgress(""); });
    return () => es.close();
  }, []);

  const unhealthy = snap ? Object.values(snap.stages).filter((s) => s.health !== "ok").length : 0;

  return (
    <div className="app">
      <header>
        <div>
          <h1>Pipeline Doctor</h1>
          <p className="muted">AI incident agent for a streaming pipeline · provider: {provider || "..."}</p>
        </div>
        <div className={`status ${unhealthy ? "bad" : "good"}`}>{unhealthy ? `${unhealthy} stages unhealthy` : "All stages healthy"}</div>
        <nav>
          <button className={tab === "live" ? "on" : ""} onClick={() => setTab("live")}>Live</button>
          <button className={tab === "evals" ? "on" : ""} onClick={() => setTab("evals")}>Evals</button>
        </nav>
      </header>

      {tab === "live" ? (
        <main className="live">
          <FaultPanel scenarios={scenarios} active={snap?.faults ?? []} busy={agentBusy}
            onInvestigate={() => api.investigate().catch((e) => alert(e.message))} />
          <div className="center">
            <PipelineMap snap={snap} />
            <ApprovalCard p={proposal} />
          </div>
          <AgentTimeline steps={steps} />
        </main>
      ) : (
        <main><Scoreboard data={board} progress={progress} busy={evalBusy} /></main>
      )}
    </div>
  );
}

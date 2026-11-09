export type Health = "ok" | "warning" | "critical";

export interface StageView {
  throughput: number;
  error_rate: number;
  p95_ms: number;
  health: Health;
}

export interface Snapshot {
  t: number;
  faults: string[];
  stages: Record<string, StageView>;
  lag: number[];
  dlq_size: number;
  edges: { source: string; target: string }[];
}

export interface Scenario {
  label: string;
  description: string;
  root_cause: string;
  fix: string;
}

export interface AgentStep {
  run_id: string;
  kind: "start" | "thought" | "tool" | "end";
  t: number;
  [k: string]: unknown;
}

export interface Proposal {
  id: string;
  status: "pending" | "approved" | "rejected";
  root_cause: string;
  evidence: string[];
  confidence: number;
  action: string;
  blast_radius: string;
  rollback: string;
  outcome?: string;
}

export interface ScoreRow {
  k: string;
  runs: number;
  cause_acc: number;
  action_acc: number;
  avg_s: number;
  avg_calls: number;
  avg_cost: number;
  violations: number;
}

export interface Scoreboard {
  batch: string | null;
  by_scenario: ScoreRow[];
  by_model: ScoreRow[];
  overall: ScoreRow | null;
}

const json = (r: Response) => {
  if (!r.ok) return r.text().then((t) => Promise.reject(new Error(t)));
  return r.json();
};

export const api = {
  state: () => fetch("/api/state").then(json),
  inject: (s: string, distractor: boolean) =>
    fetch(`/api/faults/${s}?distractor=${distractor}`, { method: "POST" }).then(json),
  clear: () => fetch("/api/faults", { method: "DELETE" }).then(json),
  investigate: () =>
    fetch("/api/investigate", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }).then(json),
  decide: (id: string, d: "approve" | "reject") => fetch(`/api/proposals/${id}/${d}`, { method: "POST" }).then(json),
  scoreboard: (): Promise<Scoreboard> => fetch("/api/evals").then(json),
  runEval: (repeats: number) =>
    fetch("/api/evals/run", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ repeats }),
    }).then(json),
};

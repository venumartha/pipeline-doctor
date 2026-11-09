import { api, type Proposal } from "../api";

export function ApprovalCard({ p }: { p: Proposal | null }) {
  if (!p) return null;
  const pending = p.status === "pending";
  return (
    <section className={`panel approval ${p.status}`}>
      <h2>Proposed fix <span className={`pill ${p.status}`}>{p.status}</span></h2>
      <dl>
        <dt>Root cause</dt><dd><b>{p.root_cause.replace(/_/g, " ")}</b> ({Math.round(p.confidence * 100)}% confident)</dd>
        <dt>Action</dt><dd><code>{p.action}</code></dd>
        <dt>Evidence</dt><dd><ul>{p.evidence.map((e, i) => <li key={i}>{e}</li>)}</ul></dd>
        <dt>Blast radius</dt><dd>{p.blast_radius}</dd>
        <dt>Rollback</dt><dd>{p.rollback}</dd>
        {p.outcome && (<><dt>Outcome</dt><dd>{p.outcome}</dd></>)}
      </dl>
      {pending && (
        <div className="row">
          <button className="ghost" onClick={() => api.decide(p.id, "reject")}>Reject</button>
          <button className="primary" onClick={() => api.decide(p.id, "approve")}>Approve</button>
        </div>
      )}
    </section>
  );
}

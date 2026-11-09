import { useMemo } from "react";
import { ReactFlow, Background, Handle, Position, type Node, type Edge, type NodeProps } from "@xyflow/react";
import type { Snapshot, StageView } from "../api";

const LAYOUT: Record<string, { x: number; y: number; label: string }> = {
  devices: { x: 0, y: 80, label: "Devices" },
  ingest: { x: 210, y: 80, label: "Ingest" },
  kafka: { x: 420, y: 80, label: "Kafka" },
  enricher: { x: 630, y: 80, label: "Enricher" },
  sink: { x: 840, y: 80, label: "API sink" },
  dlq: { x: 630, y: 260, label: "DLQ" },
};

type StageData = { label: string; view?: StageView; extra?: string };

function StageNode({ data }: NodeProps<Node<StageData>>) {
  const v = data.view;
  return (
    <div className={`stage stage-${v?.health ?? "ok"}`}>
      <Handle type="target" position={Position.Left} />
      <div className="stage-name">{data.label}</div>
      {v && (
        <div className="stage-stats">
          <span>{v.throughput.toFixed(0)} msg/s</span>
          <span>p95 {v.p95_ms >= 1000 ? `${(v.p95_ms / 1000).toFixed(1)}s` : `${v.p95_ms.toFixed(0)}ms`}</span>
          {v.error_rate > 0.001 && <span className="err">{(v.error_rate * 100).toFixed(0)}% err</span>}
        </div>
      )}
      {data.extra && <div className="stage-extra">{data.extra}</div>}
      <Handle type="source" position={Position.Right} />
      <Handle type="source" id="bottom" position={Position.Bottom} />
      <Handle type="target" id="top" position={Position.Top} />
    </div>
  );
}

const nodeTypes = { stage: StageNode };

export function PipelineMap({ snap }: { snap: Snapshot | null }) {
  const nodes: Node<StageData>[] = useMemo(
    () =>
      Object.entries(LAYOUT).map(([id, l]) => {
        let extra: string | undefined;
        if (id === "kafka" && snap) extra = `lag ${snap.lag.map((x) => x.toLocaleString()).join(" | ")}`;
        if (id === "dlq" && snap) extra = `${snap.dlq_size} messages`;
        return { id, type: "stage", position: { x: l.x, y: l.y }, data: { label: l.label, view: snap?.stages[id], extra }, draggable: false };
      }),
    [snap]
  );

  const edges: Edge[] = useMemo(
    () =>
      (snap?.edges ?? []).map((e) => {
        const target = snap!.stages[e.target];
        const toDlq = e.target === "dlq";
        const rate = snap!.stages[e.source]?.throughput ?? 0;
        return {
          id: `${e.source}-${e.target}`,
          source: e.source,
          target: e.target,
          sourceHandle: toDlq ? "bottom" : undefined,
          targetHandle: toDlq ? "top" : undefined,
          animated: toDlq ? target.throughput > 1 : rate > 1,
          label: toDlq ? undefined : `${rate.toFixed(0)}/s`,
          className: `edge-${target?.health ?? "ok"}`,
        };
      }),
    [snap]
  );

  return (
    <div className="map">
      <ReactFlow nodes={nodes} edges={edges} nodeTypes={nodeTypes} fitView proOptions={{ hideAttribution: true }}
        nodesConnectable={false} zoomOnScroll={false} panOnDrag={false}>
        <Background gap={24} />
      </ReactFlow>
    </div>
  );
}

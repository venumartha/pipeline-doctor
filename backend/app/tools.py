"""Agent tools. Five read only tools and one gated write tool.

The same functions are used in two ways:
  1. In process by the agent loop (agent.py), through TOOL_SPECS + call_tool.
  2. Over MCP by any MCP client (mcp_server.py), e.g. Claude Desktop.

Every tool returns a short summary, not raw data, to keep context small and cheap.
"""

from __future__ import annotations

import statistics
from typing import Any

from .scenarios import ACTIONS, ROOT_CAUSES
from .simulator import PARTITIONS, STAGES, Pipeline

MAX_LOG_LINES = 15
MAX_DLQ_SAMPLES = 10


def get_topology(p: Pipeline) -> dict:
    snap = p.snapshot()
    return {
        "stages": {s: v["health"] for s, v in snap["stages"].items()},
        "edges": [f"{e['source']} -> {e['target']}" for e in snap["edges"]],
    }


def get_metrics(p: Pipeline, stage: str, window_s: int = 30) -> dict:
    if stage not in STAGES:
        return {"error": f"unknown stage {stage}; valid: {STAGES}"}
    window_s = max(5, min(window_s, 120))
    h = list(p.history[stage])[-window_s:]
    if not h:
        return {"stage": stage, "note": "no data yet"}
    return {
        "stage": stage,
        "window_s": len(h),
        "throughput_avg": round(statistics.mean(x.throughput for x in h), 1),
        "error_rate_avg": round(statistics.mean(x.error_rate for x in h), 3),
        "p95_ms_avg": round(statistics.mean(x.p95_ms for x in h), 1),
        "throughput_now": round(h[-1].throughput, 1),
    }


def get_consumer_lag(p: Pipeline) -> dict:
    return {
        "group": "enricher",
        "lag_by_partition": {str(i): round(p.lag[i]) for i in range(PARTITIONS)},
        "total_lag": round(sum(p.lag)),
    }


def sample_dlq(p: Pipeline, n: int = 5) -> dict:
    n = max(1, min(n, MAX_DLQ_SAMPLES))
    items = list(p.dlq)[-n:]
    reasons: dict[str, int] = {}
    for it in p.dlq:
        reasons[it["error"]] = reasons.get(it["error"], 0) + 1
    return {"size": len(p.dlq), "top_reasons": reasons, "latest": items}


def get_recent_logs(p: Pipeline, stage: str, level: str = "WARN") -> dict:
    if stage not in STAGES:
        return {"error": f"unknown stage {stage}; valid: {STAGES}"}
    order = {"INFO": 0, "WARN": 1, "ERROR": 2}
    floor = order.get(level.upper(), 1)
    lines = [l for l in p.logs[stage] if order.get(l["level"], 0) >= floor]
    return {"stage": stage, "count": len(lines), "lines": lines[-MAX_LOG_LINES:]}


# Pending actions live in approvals.py; this tool only validates and returns the proposal.
def propose_action(
    p: Pipeline,
    root_cause: str,
    evidence: list[str],
    confidence: float,
    action: str,
    blast_radius: str,
    rollback: str,
) -> dict:
    if root_cause not in ROOT_CAUSES:
        return {"error": f"root_cause must be one of {ROOT_CAUSES}"}
    if action not in ACTIONS:
        return {"error": f"action not in allow list {list(ACTIONS)}"}
    return {
        "status": "pending_approval",
        "root_cause": root_cause,
        "evidence": evidence[:6],
        "confidence": max(0.0, min(float(confidence), 1.0)),
        "action": action,
        "blast_radius": blast_radius,
        "rollback": rollback,
    }


# ---------- specs for the LLM (Anthropic tool format) ----------
TOOL_SPECS: list[dict[str, Any]] = [
    {
        "name": "get_topology",
        "description": "List pipeline stages with their health (ok, warning, critical) and the edges between them.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_metrics",
        "description": "Average throughput (msg/s), error rate, and p95 latency for one stage over a window.",
        "input_schema": {
            "type": "object",
            "properties": {
                "stage": {"type": "string", "enum": STAGES},
                "window_s": {"type": "integer", "minimum": 5, "maximum": 120},
            },
            "required": ["stage"],
        },
    },
    {
        "name": "get_consumer_lag",
        "description": "Kafka consumer lag for the enricher group, per partition and total.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "sample_dlq",
        "description": "Dead letter queue size, counts by error reason, and the latest N messages.",
        "input_schema": {
            "type": "object",
            "properties": {"n": {"type": "integer", "minimum": 1, "maximum": MAX_DLQ_SAMPLES}},
        },
    },
    {
        "name": "get_recent_logs",
        "description": "Recent log lines for one stage at or above a level.",
        "input_schema": {
            "type": "object",
            "properties": {
                "stage": {"type": "string", "enum": STAGES},
                "level": {"type": "string", "enum": ["INFO", "WARN", "ERROR"]},
            },
            "required": ["stage"],
        },
    },
    {
        "name": "propose_action",
        "description": (
            "Final step. Submit your diagnosis and ONE fix for human approval. "
            "This never executes anything. Call it exactly once."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "root_cause": {"type": "string", "enum": ROOT_CAUSES},
                "evidence": {"type": "array", "items": {"type": "string"}, "maxItems": 6},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "action": {"type": "string", "enum": list(ACTIONS)},
                "blast_radius": {"type": "string"},
                "rollback": {"type": "string"},
            },
            "required": ["root_cause", "evidence", "confidence", "action", "blast_radius", "rollback"],
        },
    },
]

_FUNCS = {
    "get_topology": get_topology,
    "get_metrics": get_metrics,
    "get_consumer_lag": get_consumer_lag,
    "sample_dlq": sample_dlq,
    "get_recent_logs": get_recent_logs,
    "propose_action": propose_action,
}
READ_TOOLS = {"get_topology", "get_metrics", "get_consumer_lag", "sample_dlq", "get_recent_logs"}


def call_tool(p: Pipeline, name: str, args: dict) -> dict:
    fn = _FUNCS.get(name)
    if fn is None:
        return {"error": f"tool {name} is not allowed"}
    try:
        return fn(p, **(args or {}))
    except TypeError as e:
        return {"error": f"bad arguments: {e}"}

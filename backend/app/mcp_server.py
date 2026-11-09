"""Expose the pipeline tools over MCP (stdio), so any MCP client can investigate.

Run:  python -m app.mcp_server
This starts its own simulator with a fault you pick, so you can point Claude
Desktop or another MCP client at it and compare it with the built in agent.
"""

from __future__ import annotations

import os
import threading
import time

try:  # mcp 2.x
    from mcp.server.mcpserver import MCPServer as FastMCP
except ImportError:  # mcp 1.x
    from mcp.server.fastmcp import FastMCP

from . import tools
from .simulator import Pipeline

pipeline = Pipeline()
if os.getenv("FAULT"):
    pipeline.inject(os.environ["FAULT"], distractor=os.getenv("DISTRACTOR") == "1")


def _run_sim():
    while True:
        pipeline.tick()
        time.sleep(1)


mcp = FastMCP("pipeline-doctor")


@mcp.tool()
def get_topology() -> dict:
    """List pipeline stages with health and the edges between them."""
    return tools.get_topology(pipeline)


@mcp.tool()
def get_metrics(stage: str, window_s: int = 30) -> dict:
    """Average throughput, error rate, and p95 latency for one stage."""
    return tools.get_metrics(pipeline, stage, window_s)


@mcp.tool()
def get_consumer_lag() -> dict:
    """Kafka consumer lag for the enricher group, per partition."""
    return tools.get_consumer_lag(pipeline)


@mcp.tool()
def sample_dlq(n: int = 5) -> dict:
    """Dead letter queue size, reasons, and latest messages."""
    return tools.sample_dlq(pipeline, n)


@mcp.tool()
def get_recent_logs(stage: str, level: str = "WARN") -> dict:
    """Recent log lines for one stage at or above a level."""
    return tools.get_recent_logs(pipeline, stage, level)


@mcp.tool()
def propose_action(root_cause: str, evidence: list[str], confidence: float, action: str,
                   blast_radius: str, rollback: str) -> dict:
    """Submit a diagnosis and one fix for human approval. Never executes anything."""
    return tools.propose_action(pipeline, root_cause, evidence, confidence, action, blast_radius, rollback)


if __name__ == "__main__":
    threading.Thread(target=_run_sim, daemon=True).start()
    mcp.run()

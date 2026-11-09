"""The investigation loop: observe, hypothesize, verify, propose.

Bounded by MAX_TOOL_CALLS and TIME_LIMIT_S. Read tools never change state, and
propose_action only creates a pending proposal that a human must approve.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable

from .llm import Provider, cost_usd
from .simulator import Pipeline
from .tools import READ_TOOLS, TOOL_SPECS, call_tool

MAX_TOOL_CALLS = 12
TIME_LIMIT_S = 60

SYSTEM_PROMPT = """You are an on call SRE investigating an alert in an IoT event streaming pipeline.
Stages: devices -> ingest -> kafka -> enricher -> sink, with a DLQ off the enricher.

Work like this:
1. Look at the topology and the unhealthy stages.
2. Form up to 3 hypotheses about the root cause.
3. Use the read tools to confirm or rule out each one. Ignore noise that does not explain the symptoms.
4. Call propose_action exactly once with the root cause, short evidence, confidence, ONE fix, its blast radius, and rollback.

Before each tool call, write one short sentence saying what you are checking and why.
You have at most {max_calls} tool calls. Be efficient."""


@dataclass
class RunResult:
    run_id: str
    proposal: dict | None
    steps: list[dict] = field(default_factory=list)
    tool_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    duration_s: float = 0.0
    stopped_reason: str = ""
    safety_violations: int = 0


def investigate(
    pipeline: Pipeline,
    provider: Provider,
    alert: str,
    on_step: Callable[[dict], None] | None = None,
) -> RunResult:
    run = RunResult(run_id=uuid.uuid4().hex[:10], proposal=None)
    emit = on_step or (lambda s: None)
    started = time.time()
    messages: list[dict] = [{"role": "user", "content": f"ALERT: {alert}"}]
    system = SYSTEM_PROMPT.format(max_calls=MAX_TOOL_CALLS)

    def step(kind: str, **data):
        s = {"run_id": run.run_id, "kind": kind, "t": round(time.time() - started, 2), **data}
        run.steps.append(s)
        emit(s)

    step("start", alert=alert, provider=provider.name, model=provider.model)

    while True:
        if run.tool_calls >= MAX_TOOL_CALLS:
            run.stopped_reason = "tool_call_limit"
            break
        if time.time() - started > TIME_LIMIT_S:
            run.stopped_reason = "time_limit"
            break

        resp = provider.step(system, messages, TOOL_SPECS)
        run.input_tokens += resp["usage"].get("input_tokens", 0)
        run.output_tokens += resp["usage"].get("output_tokens", 0)
        messages.append({"role": "assistant", "content": resp["content"]})

        tool_results = []
        for block in resp["content"]:
            if block["type"] == "text" and block["text"].strip():
                step("thought", text=block["text"].strip())
            elif block["type"] == "tool_use":
                name, args = block["name"], block.get("input") or {}
                run.tool_calls += 1
                if name not in READ_TOOLS and name != "propose_action":
                    run.safety_violations += 1
                result = call_tool(pipeline, name, args)
                step("tool", name=name, args=args, result=result)
                if name == "propose_action" and result.get("status") == "pending_approval":
                    run.proposal = result
                tool_results.append(
                    {"type": "tool_result", "tool_use_id": block["id"], "content": json.dumps(result)}
                )

        if run.proposal:
            run.stopped_reason = "proposed"
            break
        if not tool_results:
            run.stopped_reason = "no_proposal"
            break
        messages.append({"role": "user", "content": tool_results})

    run.duration_s = round(time.time() - started, 2)
    run.cost_usd = round(cost_usd(provider, {"input_tokens": run.input_tokens, "output_tokens": run.output_tokens}), 5)
    step(
        "end",
        stopped_reason=run.stopped_reason,
        proposal=run.proposal,
        tool_calls=run.tool_calls,
        tokens={"in": run.input_tokens, "out": run.output_tokens},
        cost_usd=run.cost_usd,
        duration_s=run.duration_s,
    )
    return run

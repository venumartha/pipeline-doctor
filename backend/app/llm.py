"""LLM provider adapter.

All providers return the same shape:
    {"content": [ {type: "text", text} | {type: "tool_use", id, name, input} ],
     "stop_reason": "tool_use" | "end_turn",
     "usage": {"input_tokens": int, "output_tokens": int}}

Providers:
  mock       Rule based, no API key, deterministic. Good for demos offline and tests.
  anthropic  Claude API (needs ANTHROPIC_API_KEY).
  bedrock    Claude on Amazon Bedrock (needs AWS credentials).
"""

from __future__ import annotations

import json
import os
import uuid
from typing import Any, Protocol

# USD per million tokens. Edit to match current pricing for the model you use.
PRICES = {
    "mock": (0.0, 0.0),
    "default": (3.0, 15.0),
}


class Provider(Protocol):
    name: str
    model: str

    def step(self, system: str, messages: list[dict], tools: list[dict]) -> dict: ...


def cost_usd(provider: Provider, usage: dict) -> float:
    pin, pout = PRICES.get(provider.name if provider.name == "mock" else provider.model, PRICES["default"])
    return (usage.get("input_tokens", 0) * pin + usage.get("output_tokens", 0) * pout) / 1_000_000


# ---------------------------------------------------------------- Claude
class AnthropicProvider:
    name = "anthropic"

    def __init__(self, model: str, client: Any = None):
        import anthropic

        self.model = model
        self.client = client or anthropic.Anthropic()

    def step(self, system: str, messages: list[dict], tools: list[dict]) -> dict:
        resp = self.client.messages.create(
            model=self.model,
            max_tokens=1024,
            system=system,
            messages=messages,
            tools=tools,
        )
        content = []
        for b in resp.content:
            if b.type == "text":
                content.append({"type": "text", "text": b.text})
            elif b.type == "tool_use":
                content.append({"type": "tool_use", "id": b.id, "name": b.name, "input": b.input})
        return {
            "content": content,
            "stop_reason": resp.stop_reason,
            "usage": {"input_tokens": resp.usage.input_tokens, "output_tokens": resp.usage.output_tokens},
        }


class BedrockProvider(AnthropicProvider):
    name = "bedrock"

    def __init__(self, model: str):
        import anthropic

        super().__init__(model, client=anthropic.AnthropicBedrock())


# ---------------------------------------------------------------- Mock
class MockProvider:
    """A scripted investigator. It calls the same tools a real model would, then
    applies simple rules to the results. It exists so the demo and the eval
    harness run with no API key, and so you have a baseline to beat."""

    name = "mock"
    model = "rules-v1"

    PLAN = [
        ("get_topology", {}),
        ("get_metrics", {"stage": "devices", "window_s": 20}),
        ("get_metrics", {"stage": "ingest", "window_s": 20}),
        ("get_consumer_lag", {}),
        ("sample_dlq", {"n": 5}),
        ("get_recent_logs", {"stage": "enricher", "level": "WARN"}),
        ("get_recent_logs", {"stage": "sink", "level": "ERROR"}),
    ]

    def step(self, system: str, messages: list[dict], tools: list[dict]) -> dict:
        results = _tool_results(messages)
        usage = {"input_tokens": 0, "output_tokens": 0}
        if len(results) < len(self.PLAN):
            name, args = self.PLAN[len(results)]
            thought = f"Checking {name.replace('_', ' ')}" + (f" for {args['stage']}" if "stage" in args else "")
            return {
                "content": [
                    {"type": "text", "text": thought},
                    {"type": "tool_use", "id": f"toolu_{uuid.uuid4().hex[:12]}", "name": name, "input": args},
                ],
                "stop_reason": "tool_use",
                "usage": usage,
            }
        if any(r.get("status") == "pending_approval" for r in results):
            return {"content": [{"type": "text", "text": "Proposal submitted."}], "stop_reason": "end_turn", "usage": usage}
        return {
            "content": [
                {"type": "text", "text": "Enough evidence. Submitting diagnosis."},
                {"type": "tool_use", "id": f"toolu_{uuid.uuid4().hex[:12]}", "name": "propose_action", "input": _decide(results)},
            ],
            "stop_reason": "tool_use",
            "usage": usage,
        }


def _tool_results(messages: list[dict]) -> list[dict]:
    out = []
    for m in messages:
        if m["role"] != "user" or not isinstance(m["content"], list):
            continue
        for b in m["content"]:
            if b.get("type") == "tool_result":
                try:
                    out.append(json.loads(b["content"]))
                except (json.JSONDecodeError, TypeError):
                    out.append({})
    return out


def _decide(r: list[dict]) -> dict:
    _topo, dev, ing, lag, dlq, enr_logs, sink_logs = (r + [{}] * 7)[:7]
    reasons = " ".join(dlq.get("top_reasons", {}).keys())
    enr_text = " ".join(l["msg"] for l in enr_logs.get("lines", []))
    parts = list(lag.get("lag_by_partition", {}).values()) or [0]
    total = lag.get("total_lag", 0)

    def pick(cause, action, ev, conf, blast):
        return {
            "root_cause": cause,
            "evidence": ev,
            "confidence": conf,
            "action": action,
            "blast_radius": blast,
            "rollback": "Revert the action; all actions in the allow list are reversible.",
        }

    if sink_logs.get("count", 0) > 0 and "timed out" in json.dumps(sink_logs):
        return pick("downstream_timeout", "pause_and_retry_with_backoff",
                    [f"sink has {sink_logs['count']} timeout errors", "upstream stages healthy"], 0.85,
                    "API sink only; messages buffer in Kafka while paused")
    if "KeyError" in reasons:
        return pick("schema_break", "rollback_schema_and_replay_dlq",
                    [f"DLQ reasons: {reasons}", f"DLQ size {dlq.get('size')}"], 0.9,
                    "Producers on the new schema version; DLQ replay re-sends failed messages")
    if "ParseError" in reasons or "ParseError" in enr_text:
        return pick("poison_message", "skip_poison_message",
                    ["enricher retrying the same offset", f"DLQ reasons: {reasons}"], 0.8,
                    "One record on one partition")
    if total > 500 and max(parts) > 3 * (sorted(parts)[len(parts) // 2] + 1):
        return pick("hot_partition", "rebalance_partition_key",
                    [f"lag by partition {lag.get('lag_by_partition')}"], 0.75,
                    "Messages from one device key")
    if total > 500:
        return pick("slow_consumer", "scale_consumers",
                    [f"total lag {total} spread across partitions", "enricher batches slow"], 0.8,
                    "Enricher consumer group only")
    d, i = dev.get("throughput_avg", 0), ing.get("throughput_avg", 0)
    if d and i < 0.85 * d:
        return pick("ingest_data_loss", "replay_from_source",
                    [f"devices send {d}/s but ingest outputs {i}/s", "no errors logged at ingest"], 0.7,
                    "Messages lost between devices and ingest")
    return pick("slow_consumer", "scale_consumers", ["no strong signal; lowest risk action"], 0.2,
                "Enricher consumer group only")


def make_provider(name: str | None = None, model: str | None = None) -> Provider:
    name = (name or os.getenv("LLM_PROVIDER", "mock")).lower()
    model = model or os.getenv("LLM_MODEL", "claude-sonnet-4-5")
    if name == "anthropic":
        return AnthropicProvider(model)
    if name == "bedrock":
        return BedrockProvider(os.getenv("BEDROCK_MODEL_ID", model))
    return MockProvider()

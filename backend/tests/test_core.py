import os
import random
import tempfile

os.environ.setdefault("TRACE_DB", os.path.join(tempfile.mkdtemp(), "t.db"))

import pytest  # noqa: E402

from app.agent import investigate  # noqa: E402
from app.llm import MockProvider  # noqa: E402
from app.scenarios import SCENARIOS  # noqa: E402
from app.simulator import Pipeline  # noqa: E402
from app.tools import call_tool  # noqa: E402


def warm(p, n=30):
    for _ in range(n):
        p.tick()


def test_healthy_pipeline_is_ok():
    p = Pipeline()
    warm(p)
    assert all(v["health"] == "ok" for v in p.snapshot()["stages"].values())


def test_unknown_tool_is_blocked():
    assert "error" in call_tool(Pipeline(), "delete_topic", {})


def test_propose_rejects_actions_outside_allow_list():
    r = call_tool(Pipeline(), "propose_action", {
        "root_cause": "slow_consumer", "evidence": [], "confidence": 0.5,
        "action": "drop_all_messages", "blast_radius": "", "rollback": ""})
    assert "error" in r


@pytest.mark.parametrize("scenario", list(SCENARIOS))
@pytest.mark.parametrize("distractor", [False, True])
def test_mock_agent_finds_each_fault(scenario, distractor):
    p = Pipeline(rng=random.Random(1))
    p.inject(scenario, distractor)
    warm(p)
    run = investigate(p, MockProvider(), "test alert")
    assert run.proposal is not None
    assert run.proposal["root_cause"] == SCENARIOS[scenario]["root_cause"]
    assert run.tool_calls <= 12
    assert run.safety_violations == 0

"""Eval harness: run every scenario (with and without a distractor) N times and score it."""

from __future__ import annotations

import random
import uuid
from typing import Callable

from .agent import investigate
from .llm import Provider
from .scenarios import SCENARIOS
from .simulator import Pipeline
from .traces import save_run, scoreboard

WARMUP_TICKS = 30


def run_eval(
    provider: Provider,
    repeats: int = 5,
    scenarios: list[str] | None = None,
    with_distractors: bool = True,
    on_progress: Callable[[dict], None] | None = None,
) -> dict:
    batch = f"eval-{uuid.uuid4().hex[:8]}"
    names = scenarios or list(SCENARIOS)
    variants = [False, True] if with_distractors else [False]
    total = len(names) * len(variants) * repeats
    done = 0
    for name in names:
        for distractor in variants:
            for i in range(repeats):
                p = Pipeline(rng=random.Random(hash((name, distractor, i)) & 0xFFFF))
                p.inject(name, distractor=distractor)
                for _ in range(WARMUP_TICKS):
                    p.tick()
                alert = f"Pipeline alert: unhealthy stages {[s for s, v in p.snapshot()['stages'].items() if v['health'] != 'ok'] or 'none flagged, throughput anomaly'}"
                run = investigate(p, provider, alert)
                save_run(run, provider, source="eval", scenario=name, distractor=distractor,
                         expected=SCENARIOS[name], eval_batch=batch)
                done += 1
                if on_progress:
                    on_progress({"batch": batch, "done": done, "total": total, "scenario": name})
    return scoreboard(batch)

"""SQLite trace store. Every run is saved so it can be replayed and scored."""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path

DB_PATH = Path(os.getenv("TRACE_DB", Path(__file__).resolve().parent.parent / "traces.db"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  created REAL,
  source TEXT,            -- 'ui' or 'eval'
  eval_batch TEXT,
  scenario TEXT,
  distractor INTEGER,
  provider TEXT,
  model TEXT,
  expected_cause TEXT,
  predicted_cause TEXT,
  expected_action TEXT,
  predicted_action TEXT,
  correct_cause INTEGER,
  correct_action INTEGER,
  tool_calls INTEGER,
  input_tokens INTEGER,
  output_tokens INTEGER,
  cost_usd REAL,
  duration_s REAL,
  stopped_reason TEXT,
  safety_violations INTEGER,
  steps TEXT
);
"""


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    c.executescript(SCHEMA)
    return c


def save_run(run, provider, *, source: str, scenario: str | None, distractor: bool,
             expected: dict | None, eval_batch: str | None = None) -> None:
    p = run.proposal or {}
    exp_cause = (expected or {}).get("root_cause")
    exp_action = (expected or {}).get("fix")
    with _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                run.run_id, time.time(), source, eval_batch, scenario, int(distractor),
                provider.name, provider.model, exp_cause, p.get("root_cause"), exp_action, p.get("action"),
                None if exp_cause is None else int(p.get("root_cause") == exp_cause),
                None if exp_action is None else int(p.get("action") == exp_action),
                run.tool_calls, run.input_tokens, run.output_tokens, run.cost_usd, run.duration_s,
                run.stopped_reason, run.safety_violations, json.dumps(run.steps),
            ),
        )


def scoreboard(eval_batch: str | None = None) -> dict:
    with _conn() as c:
        if eval_batch is None:
            row = c.execute("SELECT eval_batch FROM runs WHERE source='eval' ORDER BY created DESC LIMIT 1").fetchone()
            if not row:
                return {"batch": None, "by_scenario": [], "by_model": [], "overall": None}
            eval_batch = row["eval_batch"]
        q = """SELECT {key} AS k, COUNT(*) AS runs,
                      AVG(correct_cause) AS cause_acc, AVG(correct_action) AS action_acc,
                      AVG(duration_s) AS avg_s, AVG(tool_calls) AS avg_calls,
                      AVG(cost_usd) AS avg_cost, SUM(safety_violations) AS violations
               FROM runs WHERE eval_batch=? GROUP BY {key} ORDER BY {key}"""
        by_s = [dict(r) for r in c.execute(q.format(key="scenario || CASE distractor WHEN 1 THEN '+noise' ELSE '' END"), (eval_batch,))]
        by_m = [dict(r) for r in c.execute(q.format(key="provider || ':' || model"), (eval_batch,))]
        overall = dict(c.execute(q.format(key="eval_batch"), (eval_batch,)).fetchone())
    return {"batch": eval_batch, "by_scenario": by_s, "by_model": by_m, "overall": overall}

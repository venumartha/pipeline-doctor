"""API server: runs the simulator, the agent, approvals, evals, and a live event stream."""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .agent import investigate
from .evals import run_eval
from .llm import make_provider
from .scenarios import ACTION_FIXES, ACTIONS, SCENARIOS
from .simulator import Pipeline
from .traces import save_run, scoreboard

pipeline = Pipeline()
proposals: dict[str, dict] = {}
subscribers: set[asyncio.Queue] = set()
state = {"agent_busy": False, "eval_busy": False}
loop_ref: dict = {}


def publish(event: str, data: dict) -> None:
    """Thread safe fan out to every SSE client."""
    loop = loop_ref.get("loop")
    if not loop:
        return
    msg = f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"
    for q in list(subscribers):
        loop.call_soon_threadsafe(q.put_nowait, msg)


async def ticker():
    while True:
        pipeline.tick()
        publish("metrics", pipeline.snapshot())
        await asyncio.sleep(float(os.getenv("TICK_SECONDS", "1")))


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop_ref["loop"] = asyncio.get_running_loop()
    task = asyncio.create_task(ticker())
    yield
    task.cancel()


app = FastAPI(title="Pipeline Doctor", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ---------------------------------------------------------------- read
@app.get("/api/state")
def get_state():
    return {
        "snapshot": pipeline.snapshot(),
        "scenarios": SCENARIOS,
        "actions": ACTIONS,
        "proposals": list(proposals.values()),
        "busy": state,
        "provider": os.getenv("LLM_PROVIDER", "mock"),
    }


@app.get("/api/stream")
async def stream():
    q: asyncio.Queue = asyncio.Queue()
    subscribers.add(q)

    async def gen():
        try:
            yield f"event: metrics\ndata: {json.dumps(pipeline.snapshot())}\n\n"
            while True:
                yield await q.get()
        finally:
            subscribers.discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream")


# ---------------------------------------------------------------- faults
@app.post("/api/faults/{scenario}")
def inject(scenario: str, distractor: bool = False):
    if scenario not in SCENARIOS:
        raise HTTPException(404, f"unknown scenario {scenario}")
    pipeline.inject(scenario, distractor)
    return {"faults": sorted(pipeline.faults)}


@app.delete("/api/faults")
def clear_faults():
    pipeline.clear()
    pipeline.lag = [0.0] * len(pipeline.lag)
    pipeline.dlq.clear()
    return {"faults": []}


# ---------------------------------------------------------------- agent
class InvestigateReq(BaseModel):
    provider: str | None = None
    model: str | None = None


@app.post("/api/investigate")
async def start_investigation(req: InvestigateReq | None = None):
    if state["agent_busy"]:
        raise HTTPException(409, "an investigation is already running")
    req = req or InvestigateReq()
    provider = make_provider(req.provider, req.model)
    snap = pipeline.snapshot()
    bad = [s for s, v in snap["stages"].items() if v["health"] != "ok"]
    alert = f"Unhealthy stages: {bad}" if bad else "Throughput anomaly reported by a downstream team"
    scenario = sorted(pipeline.faults)[0] if pipeline.faults else None

    def work():
        state["agent_busy"] = True
        try:
            run = investigate(pipeline, provider, alert, on_step=lambda s: publish("agent", s))
            save_run(run, provider, source="ui", scenario=scenario, distractor=pipeline.distractor,
                     expected=SCENARIOS.get(scenario) if scenario else None)
            if run.proposal:
                prop = {**run.proposal, "id": run.run_id, "status": "pending"}
                proposals[run.run_id] = prop
                publish("proposal", prop)
        finally:
            state["agent_busy"] = False

    asyncio.get_running_loop().run_in_executor(None, work)
    return {"started": True, "alert": alert}


@app.post("/api/proposals/{pid}/{decision}")
def decide(pid: str, decision: str):
    prop = proposals.get(pid)
    if not prop:
        raise HTTPException(404, "no such proposal")
    if prop["status"] != "pending":
        raise HTTPException(409, f"already {prop['status']}")
    if decision not in ("approve", "reject"):
        raise HTTPException(400, "decision must be approve or reject")
    prop["status"] = "approved" if decision == "approve" else "rejected"
    if decision == "approve":
        fixed = ACTION_FIXES.get(prop["action"])
        if fixed in pipeline.faults:
            pipeline.clear(fixed)
            pipeline.lag = [0.0] * len(pipeline.lag)
            prop["outcome"] = f"fault {fixed} resolved"
        else:
            prop["outcome"] = "action applied, but the fault is still active"
    publish("proposal", prop)
    return prop


# ---------------------------------------------------------------- evals
class EvalReq(BaseModel):
    provider: str | None = None
    model: str | None = None
    repeats: int = 3
    with_distractors: bool = True


@app.get("/api/evals")
def get_scoreboard():
    return scoreboard()


@app.post("/api/evals/run")
async def start_eval(req: EvalReq):
    if state["eval_busy"]:
        raise HTTPException(409, "an eval is already running")
    provider = make_provider(req.provider, req.model)

    def work():
        state["eval_busy"] = True
        try:
            result = run_eval(provider, repeats=req.repeats, with_distractors=req.with_distractors,
                              on_progress=lambda p: publish("eval_progress", p))
            publish("eval_done", result)
        finally:
            state["eval_busy"] = False

    asyncio.get_running_loop().run_in_executor(None, work)
    return {"started": True}

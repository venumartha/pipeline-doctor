"""In memory simulation of an IoT event streaming pipeline.

Stages: devices -> ingest -> kafka -> enricher -> sink, with a DLQ off the enricher.
Every tick (1 simulated second) messages flow through the stages. Active faults
change how each stage behaves, which shows up in metrics, lag, logs, and the DLQ.
"""

from __future__ import annotations

import random
import time
from collections import deque
from dataclasses import dataclass, field

STAGES = ["devices", "ingest", "kafka", "enricher", "sink", "dlq"]
EDGES = [
    ("devices", "ingest"),
    ("ingest", "kafka"),
    ("kafka", "enricher"),
    ("enricher", "sink"),
    ("enricher", "dlq"),
]
PARTITIONS = 4
BASE_RATE = 200  # messages per second from devices
ENRICHER_CAPACITY = 260  # messages per second across all partitions
HISTORY = 120  # seconds of metrics kept per stage


@dataclass
class StageMetrics:
    throughput: float = 0.0  # messages out per second
    error_rate: float = 0.0  # 0..1
    p95_ms: float = 0.0


@dataclass
class Pipeline:
    faults: set[str] = field(default_factory=set)
    distractor: bool = False
    tick_count: int = 0
    lag: list[float] = field(default_factory=lambda: [0.0] * PARTITIONS)
    history: dict[str, deque] = field(
        default_factory=lambda: {s: deque(maxlen=HISTORY) for s in STAGES}
    )
    logs: dict[str, deque] = field(
        default_factory=lambda: {s: deque(maxlen=200) for s in STAGES}
    )
    dlq: deque = field(default_factory=lambda: deque(maxlen=500))
    rng: random.Random = field(default_factory=lambda: random.Random(7))

    # ---------- faults ----------
    def inject(self, scenario: str, distractor: bool = False) -> None:
        self.faults.add(scenario)
        self.distractor = self.distractor or distractor

    def clear(self, scenario: str | None = None) -> None:
        if scenario is None:
            self.faults.clear()
            self.distractor = False
        else:
            self.faults.discard(scenario)

    # ---------- simulation ----------
    def _log(self, stage: str, level: str, msg: str) -> None:
        self.logs[stage].append({"t": self.tick_count, "level": level, "msg": msg})

    def tick(self) -> dict[str, StageMetrics]:
        self.tick_count += 1
        r = self.rng
        f = self.faults
        jitter = lambda: r.uniform(0.95, 1.05)  # noqa: E731
        m: dict[str, StageMetrics] = {s: StageMetrics() for s in STAGES}

        # devices
        produced = BASE_RATE * jitter()
        if "hot_partition" in f:
            produced *= 1.6  # one device floods
        m["devices"] = StageMetrics(produced, 0.0, 5 * jitter())

        # ingest
        ingested = produced
        if "silent_drop" in f:
            ingested = produced * 0.7  # 30% vanish with no error
        m["ingest"] = StageMetrics(ingested, 0.0, 12 * jitter())
        if self.distractor:
            m["ingest"].p95_ms = 140 * jitter()
            if self.tick_count % 3 == 0:
                self._log("ingest", "WARN", "GC pause 120ms on ingest-1 (heap 71%)")

        # kafka partitions
        shares = [0.25] * PARTITIONS
        if "hot_partition" in f:
            shares = [0.1, 0.1, 0.7, 0.1]
        incoming = [ingested * s for s in shares]
        m["kafka"] = StageMetrics(ingested, 0.0, 3 * jitter())

        # enricher
        cap_total = ENRICHER_CAPACITY
        if "consumer_lag" in f:
            cap_total = ENRICHER_CAPACITY / 10
            if self.tick_count % 4 == 0:
                self._log("enricher", "WARN", "batch processing took 9.8s (expected <1s)")
        if "poison_message" in f:
            cap_total = ENRICHER_CAPACITY * 0.25  # stuck retrying one record
            self._log(
                "enricher",
                "ERROR",
                "ParseError at partition 1 offset 88412: unexpected byte 0x00, retrying (attempt %d)"
                % (self.tick_count % 50 + 1),
            )
            if self.tick_count % 5 == 0:
                self.dlq.append(
                    {"t": self.tick_count, "partition": 1, "offset": 88412,
                     "error": "ParseError: unexpected byte 0x00"}
                )
        cap_part = cap_total / PARTITIONS
        consumed = 0.0
        for i in range(PARTITIONS):
            available = self.lag[i] + incoming[i]
            take = min(available, cap_part)
            self.lag[i] = available - take
            consumed += take

        failed = 0.0
        if "bad_schema" in f:
            failed = consumed * 0.6
            for _ in range(3):
                self.dlq.append(
                    {"t": self.tick_count, "partition": r.randrange(PARTITIONS),
                     "error": "KeyError: 'device_id' (producer sent 'deviceId')"}
                )
            self._log("enricher", "ERROR", "schema validation failed: missing field device_id")
        enriched = consumed - failed
        m["enricher"] = StageMetrics(
            enriched,
            failed / consumed if consumed else 0.0,
            (9000 if "consumer_lag" in f else 40) * jitter(),
        )
        m["dlq"] = StageMetrics(failed + (0.2 if "poison_message" in f else 0.0), 0.0, 0.0)

        # sink (downstream API)
        sink_ok, sink_err, sink_p95 = enriched, 0.0, 80 * jitter()
        if "sink_timeout" in f:
            sink_ok, sink_err, sink_p95 = enriched * 0.15, enriched * 0.85, 5000 * jitter()
            self._log("sink", "ERROR", "POST /v1/events timed out after 5000ms, will retry")
        m["sink"] = StageMetrics(
            sink_ok, sink_err / enriched if enriched else 0.0, sink_p95
        )

        for s in STAGES:
            self.history[s].append(m[s])
        return m

    # ---------- read views used by tools and the UI ----------
    def snapshot(self) -> dict:
        last = {s: (self.history[s][-1] if self.history[s] else StageMetrics()) for s in STAGES}
        return {
            "t": self.tick_count,
            "wall": time.time(),
            "faults": sorted(self.faults),
            "stages": {
                s: {
                    "throughput": round(last[s].throughput, 1),
                    "error_rate": round(last[s].error_rate, 3),
                    "p95_ms": round(last[s].p95_ms, 1),
                    "health": self.health(s),
                }
                for s in STAGES
            },
            "lag": [round(x) for x in self.lag],
            "dlq_size": len(self.dlq),
            "edges": [{"source": a, "target": b} for a, b in EDGES],
        }

    def health(self, stage: str) -> str:
        h = self.history[stage]
        if not h:
            return "ok"
        cur = h[-1]
        if stage == "enricher" and sum(self.lag) > 2000:
            return "critical"
        if cur.error_rate > 0.3 or cur.p95_ms > 3000:
            return "critical"
        if cur.error_rate > 0.05 or cur.p95_ms > 1000:
            return "warning"
        if stage == "dlq" and cur.throughput > 1:
            return "warning"
        return "ok"

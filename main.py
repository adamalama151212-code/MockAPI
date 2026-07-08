"""
main.py - Mock API (FastAPI + Uvicorn) serving a polymorphic telemetry stream.

Runs on EC2 and is polled by Databricks in pull mode. Each GET returns
up to `limit` events (default 1). Every event is drawn from a randomly chosen,
not-yet-exhausted source (uniform), wrapped in the polymorphic envelope:
    {event_type, device_id, timestamp, event_ts?, payload}

Four sources:
    SYSTEM_METRICS       <- Metrics.jsonl
    AI_DETECTION_BOX     <- Dections.jsonl
    CAMERA_HEALTH        <- Camery_health.jsonl  
    RECORDINGS_METADATA  <- frigate.db recordings

Consumption model: each record is served exactly once. Cursors advance and are
persisted to offsets.json, so a restart resumes where it left off. When all four
sources are exhausted, the response is {"events": [], "done": true}.
"""

import json
import os
import random
import sqlite3
import threading
from array import array

from fastapi import FastAPI, Query

METRICS_FILE = "metryki.jsonl"
DETECTIONS_FILE = "detekcje.jsonl"
HEALTH_FILE = "kamery_health.jsonl"
FRIGATE_DB = "frigate.db"
CAM_DEVICE_MAP = "camera_device_map.json"
OFFSETS_FILE = "offsets.json"

MAX_LIMIT = 5000  # cap on records returned per request


# Sources

class FileLineSource:
    """Reads a ready-to-serve .jsonl file line by line. Cursor = byte offset,
    so restarts resume in O(1) via seek(). Each line is already a full envelope."""

    def __init__(self, event_type, path, start_offset=0):
        self.event_type = event_type
        self.path = path
        self._f = open(path, "r", encoding="utf-8")
        self._f.seek(start_offset)
        self.exhausted = False

    def next(self):
        if self.exhausted:
            return None
        while True:
            line = self._f.readline()
            if line == "":            # EOF
                self.exhausted = True
                return None
            line = line.strip()
            if line:
                return json.loads(line)

    @property
    def offset(self):
        return self._f.tell()

    def reset(self):
        self._f.seek(0)
        self.exhausted = False

    def close(self):
        self._f.close()


class RecordingsSource:
    """Serves the frigate.db recordings table live by RANDOM sampling of rowid.

    Sequential cursors (rowid or start_time) don't work here: the table stores all SD
    segments first, and HD only exists in the last ~10 days / high rowids, so any small
    sequential sample is SD-only. Random sampling makes every sample representative of the
    whole table (HD/SD ratio, all cameras, full period) - like tailing a large live topic.
    Effectively unbounded (never 'done'); the client bounds the pull. Real timestamps kept."""

    event_type = "RECORDINGS_METADATA"

    def __init__(self, db_path, cam2qnap, start=None):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self.cam2qnap = cam2qnap
        # rowids are non-contiguous (gaps), so we load them once (~32 MB as a 64-bit array)
        # and sample uniformly OVER ROWS - a plain random rowid range would be gap-biased.
        self._rowids = array("q", (r[0] for r in self._conn.execute("SELECT rowid FROM recordings")))
        self._rng = random.Random()
        self.served = int(start) if isinstance(start, (int, float)) else 0
        self.exhausted = len(self._rowids) == 0

    def next(self):
        if self.exhausted:
            return None
        rid = self._rng.choice(self._rowids)
        row = self._conn.execute(
            "SELECT rowid AS _rid, camera, path, start_time, duration, segment_size "
            "FROM recordings WHERE rowid = ?",
            (rid,),
        ).fetchone()
        self.served += 1
        cam = row["camera"]
        ts = int(row["start_time"])
        return {
            "event_type": self.event_type,
            "device_id": self.cam2qnap.get(cam, "qnap_unknown"),
            "timestamp": ts,
            "event_ts": ts,
            "payload": {
                "camera": cam,
                "segment_size_mb": row["segment_size"],
                "duration_s": row["duration"],
                "path": row["path"],
            },
        }

    @property
    def offset(self):
        return self.served

    def reset(self):
        self.served = 0
        self.exhausted = len(self._rowids) == 0


# State: sources + persistent offsets

_LOCK = threading.Lock()

with open(CAM_DEVICE_MAP, "r", encoding="utf-8") as _f:
    CAM2QNAP = json.load(_f)


def _load_offsets():
    if os.path.exists(OFFSETS_FILE):
        with open(OFFSETS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _build_sources(offsets):
    return {
        "SYSTEM_METRICS": FileLineSource(
            "SYSTEM_METRICS", METRICS_FILE, offsets.get("SYSTEM_METRICS", 0)),
        "AI_DETECTION_BOX": FileLineSource(
            "AI_DETECTION_BOX", DETECTIONS_FILE, offsets.get("AI_DETECTION_BOX", 0)),
        "CAMERA_HEALTH": FileLineSource(
            "CAMERA_HEALTH", HEALTH_FILE, offsets.get("CAMERA_HEALTH", 0)),
        "RECORDINGS_METADATA": RecordingsSource(
            FRIGATE_DB, CAM2QNAP, offsets.get("RECORDINGS_METADATA")),
    }


SOURCES = _build_sources(_load_offsets())


def _save_offsets():
    data = {name: src.offset for name, src in SOURCES.items()}
    tmp = OFFSETS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, OFFSETS_FILE)


# API

app = FastAPI(title="Polymorphic Mock Telemetry API")


@app.get("/api/v1/telemetry")
def telemetry(limit: int = Query(1, ge=1, le=MAX_LIMIT)):
    """Return up to `limit` events, each from a random non-exhausted source.
    Advances and persists cursors. Sets done=true once every source is drained."""
    with _LOCK:
        events = []
        for _ in range(limit):
            available = [s for s in SOURCES.values() if not s.exhausted]
            if not available:
                break
            rec = random.choice(available).next()
            if rec is not None:
                events.append(rec)
        _save_offsets()
        done = all(s.exhausted for s in SOURCES.values())
    return {"events": events, "count": len(events), "done": done}


@app.get("/api/v1/status")
def status():
    """Cursor position and exhaustion per source (for monitoring)."""
    with _LOCK:
        return {
            "sources": {
                name: {"offset": src.offset, "exhausted": src.exhausted}
                for name, src in SOURCES.items()
            },
            "done": all(s.exhausted for s in SOURCES.values()),
        }


@app.post("/api/v1/reset")
def reset():
    """Rewind every source to the beginning and clear persisted offsets."""
    global SOURCES
    with _LOCK:
        for src in SOURCES.values():
            src.reset()
        _save_offsets()
    return {"status": "reset"}


@app.get("/")
def root():
    return {"service": "mock-telemetry", "endpoint": "/api/v1/telemetry?limit=N"}

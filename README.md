# MockAPI - Telemetry Mock API on AWS

A small **FastAPI service on AWS (EC2)** that serves a polymorphic
telemetry stream built from **three `.jsonl` files and one SQLite database** (`frigate.db`).

The data is a mix of:
- **data from dev QNAP NAS servers** (system metrics) and a **Frigate NVR**
  (recording metadata) - dev hardware, and the exported files carry no sensitive data,
- **synthetic data** (AI object detections and camera health/ping events).

The files and database live in **S3** and are pulled onto the EC2 instance (read-only IAM
role - no credentials on the box). A **Databricks** notebook then pulls the stream over HTTP,
loads it into Delta tables, and builds a dashboard.

## What it serves

Every `GET /api/v1/telemetry?limit=N` returns a batch of events sharing one envelope
(`event_type, device_id, timestamp, payload`), randomly drawn from four sources:

| Type                  | Backed by                        | Origin                    |
|-----------------------|----------------------------------|---------------------------|
| `SYSTEM_METRICS`      | `Metrics.jsonl`                  | QNAP (sanitized)          |
| `RECORDINGS_METADATA` | `frigate.db` (recordings, live)  | Frigate (sanitized)       |
| `AI_DETECTION_BOX`    | `Detection.jsonl`                | synthetic                 |
| `CAMERA_HEALTH`       | `camera_health.jsonl`            | synthetic                 |

Other endpoints: `GET /api/v1/status` (cursor state), `POST /api/v1/reset` (rewind).

## Flow

```
S3 (data files + frigate.db)  ->  EC2 (FastAPI, systemd: mock-api, :8000)  ->  Databricks (Delta + dashboard)
```

Large data is never committed to Git - only the code lives here (`main.py`,
`requirements.txt`, `deploy/mock-api.service`, `camera_dim.csv` and the Databricks notebook).


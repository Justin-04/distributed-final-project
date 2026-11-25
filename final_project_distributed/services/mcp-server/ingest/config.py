"""Configuration for the distributed ingestion pipeline (all overridable by env vars)."""

import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6380")

# ── Streams & consumer groups ────────────────────────────────────────────────
JOBS_STREAM = "ingest:jobs"          # one message per uploaded document
PAGES_STREAM = "ingest:pages"        # one message per page to process
DLQ_STREAM = "ingest:dlq"            # pages that failed MAX_ATTEMPTS times
EVENTS_CHANNEL = "ingest:events"     # pub/sub: document.indexed, job.failed

SPLITTER_GROUP = "splitters"
PAGE_GROUP = "page-workers"

# ── Keys ─────────────────────────────────────────────────────────────────────
JOB_KEY = "ingest:job:{job_id}"                  # hash: status, counters, timings
JOB_DONE_PAGES_KEY = "ingest:job:{job_id}:done"  # set: pages finished (idempotent count)
JOB_WORKERS_KEY = "ingest:job:{job_id}:workers"  # hash: worker -> pages processed
JOB_IMAGE_CLAIM_KEY = "ingest:job:{job_id}:img:{xref}"  # image de-dup claim (value = page)
JOBS_INDEX_KEY = "ingest:jobs:index"             # zset: job_id by creation time
ACTIVE_SOURCE_KEY = "ingest:active:{source}"     # lock: one active job per document
WORKER_HEARTBEAT_KEY = "ingest:worker:{name}"    # hash with TTL: live workers

# ── Reliability ──────────────────────────────────────────────────────────────
MAX_ATTEMPTS = int(os.getenv("INGEST_MAX_ATTEMPTS", "3"))
# A page message not acknowledged for this long is considered abandoned (worker
# crashed) and is reclaimed by another worker with XAUTOCLAIM.
RECLAIM_IDLE_MS = int(os.getenv("INGEST_RECLAIM_IDLE_MS", "120000"))
RECLAIM_INTERVAL_S = float(os.getenv("INGEST_RECLAIM_INTERVAL_S", "15"))
HEARTBEAT_TTL_S = 30
JOB_TTL_S = 7 * 24 * 3600  # job metadata kept for a week

# ── Throughput / OpenAI rate limits (shared by ALL workers through Redis) ────
CAPTION_RPM = int(os.getenv("INGEST_CAPTION_RPM", "200"))
EMBED_RPM = int(os.getenv("INGEST_EMBED_RPM", "1000"))
DEFAULT_PAGE_THREADS = int(os.getenv("INGEST_PAGE_THREADS", "4"))
DEFAULT_IMAGE_THREADS = int(os.getenv("INGEST_IMAGE_THREADS", "8"))

# ── Storage ──────────────────────────────────────────────────────────────────
RAW_PDF_S3_PREFIX = "raw/"
# Fallback when S3 is unavailable (only works when all workers share a disk)
LOCAL_BLOB_DIR = os.getenv("INGEST_LOCAL_BLOB_DIR", os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "ingest_blobs"))

# Images extracted from the manuals (diagrams, page renders). The workers write them and the
# MCP server serves them (GET /images/<file>), so this folder must be shared by both: a bind
# mount in docker-compose (./data/images on the host). Answers link to the images through
# Agent A (/api/v1/images/<file>), so they no longer depend on S3.
IMAGES_DIR = os.getenv("INGEST_IMAGES_DIR", os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "images"))
# Also copy each image to S3 (off by default: the bucket credentials are not available)
UPLOAD_IMAGES_TO_S3 = os.getenv("INGEST_UPLOAD_IMAGES_TO_S3", "false").lower() == "true"

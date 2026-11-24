"""
Job lifecycle (producer side + status queries).

Job states:  queued → splitting → processing → completed | completed_with_errors | failed | cancelled
"""

import time
import uuid
import logging

from ingest import config, blob
from ingest.bus import get_redis, ensure_group, publish_event

logger = logging.getLogger(__name__)

TERMINAL_STATES = {"completed", "completed_with_errors", "failed", "cancelled"}


class DuplicateJobError(Exception):
    def __init__(self, job_id: str):
        super().__init__(f"An ingestion job is already running for this document ({job_id})")
        self.job_id = job_id


def create_job(pdf_bytes: bytes, filename: str, drone_model: str, caption_images: bool = True) -> dict:
    """
    Producer: store the PDF, create the job record, publish `ingest.requested`.
    Returns immediately — the heavy work is done asynchronously by the workers.
    """
    r = get_redis()
    job_id = uuid.uuid4().hex[:12]

    # Concurrency control: only one active job per document. SET NX is atomic, so two
    # admins uploading the same manual at the same time cannot start two jobs.
    lock_key = config.ACTIVE_SOURCE_KEY.format(source=filename)
    if not r.set(lock_key, job_id, nx=True, ex=6 * 3600):
        raise DuplicateJobError(r.get(lock_key) or "unknown")

    try:
        uri = blob.put_pdf(job_id, filename, pdf_bytes)
        now = time.time()
        job_key = config.JOB_KEY.format(job_id=job_id)
        r.hset(job_key, mapping={
            "job_id": job_id,
            "filename": filename,
            "drone_model": drone_model,
            "caption_images": "1" if caption_images else "0",
            "blob_uri": uri,
            "size_bytes": len(pdf_bytes),
            "status": "queued",
            "created_at": now,
            "total_pages": 0,
            "pages_finished": 0,
            "pages_succeeded": 0,
            "pages_failed": 0,
            "chunks": 0,
            "images": 0,
        })
        r.expire(job_key, config.JOB_TTL_S)
        r.zadd(config.JOBS_INDEX_KEY, {job_id: now})

        ensure_group(config.JOBS_STREAM, config.SPLITTER_GROUP)
        r.xadd(config.JOBS_STREAM, {"job_id": job_id})
        logger.info(f"Job {job_id} queued: '{filename}' ({len(pdf_bytes)} bytes, blob={uri})")
        return get_job(job_id)
    except Exception:
        r.delete(lock_key)
        raise


def get_job(job_id: str) -> dict | None:
    r = get_redis()
    job = r.hgetall(config.JOB_KEY.format(job_id=job_id))
    if not job:
        return None

    def _f(name, default=0.0):
        try:
            return float(job.get(name, default))
        except (TypeError, ValueError):
            return default

    total = int(_f("total_pages"))
    finished = int(_f("pages_finished"))
    created = _f("created_at")
    end = _f("finished_at") or time.time()
    workers = r.hgetall(config.JOB_WORKERS_KEY.format(job_id=job_id))

    return {
        "job_id": job_id,
        "filename": job.get("filename"),
        "drone_model": job.get("drone_model"),
        "status": job.get("status"),
        "error": job.get("error"),
        "total_pages": total,
        "pages_finished": finished,
        "pages_succeeded": int(_f("pages_succeeded")),
        "pages_failed": int(_f("pages_failed")),
        "progress": round(finished / total, 3) if total else 0.0,
        "chunks": int(_f("chunks")),
        "images": int(_f("images")),
        "workers": {w: int(n) for w, n in workers.items()},
        "timings": {
            "elapsed_seconds": round(end - created, 2) if created else None,
            "queue_wait_seconds": round(_f("split_started_at") - created, 2) if job.get("split_started_at") else None,
            "split_seconds": round(_f("split_done_at") - _f("split_started_at"), 2) if job.get("split_done_at") else None,
            # Summed across all workers (CPU-time-like): compare with elapsed to see the parallelism
            "caption_seconds_sum": round(_f("caption_seconds"), 2),
            "embed_seconds_sum": round(_f("embed_seconds"), 2),
            "upsert_seconds_sum": round(_f("upsert_seconds"), 2),
            "s3_seconds_sum": round(_f("s3_seconds"), 2),
        },
        "created_at": created,
        "finished_at": _f("finished_at") or None,
    }


def list_jobs(limit: int = 20) -> list[dict]:
    r = get_redis()
    ids = r.zrevrange(config.JOBS_INDEX_KEY, 0, limit - 1)
    jobs = [get_job(j) for j in ids]
    return [j for j in jobs if j]


def cancel_jobs_for_source(filename: str) -> str | None:
    """
    Called when a document is deleted while it is being ingested: mark the job
    cancelled so workers stop writing chunks for it.
    """
    r = get_redis()
    lock_key = config.ACTIVE_SOURCE_KEY.format(source=filename)
    job_id = r.get(lock_key)
    if not job_id:
        return None
    job_key = config.JOB_KEY.format(job_id=job_id)
    if r.hget(job_key, "status") not in TERMINAL_STATES:
        r.hset(job_key, mapping={"status": "cancelled", "finished_at": time.time()})
        publish_event("job.cancelled", job_id=job_id, filename=filename)
        logger.info(f"Job {job_id} cancelled (document '{filename}' deleted)")
    r.delete(lock_key)
    return job_id


def is_cancelled(job_id: str) -> bool:
    return get_redis().hget(config.JOB_KEY.format(job_id=job_id), "status") == "cancelled"


def live_workers() -> list[dict]:
    r = get_redis()
    workers = []
    for key in r.scan_iter("ingest:worker:*"):
        data = r.hgetall(key)
        if data:
            workers.append(data)
    return sorted(workers, key=lambda w: w.get("name", ""))

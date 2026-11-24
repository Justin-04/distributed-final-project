"""
Ingestion worker — a separate, horizontally scalable process.

Roles
  splitter : consumes `ingest:jobs`, opens the PDF, publishes one task per page to `ingest:pages`
  page     : consumes `ingest:pages` (consumer group → each page goes to exactly one worker),
             extracts the page, captions its images concurrently, chunks, embeds, upserts to Qdrant
  all      : both roles in one process (convenient for local runs)

Run as many page workers as needed, on any machine that can reach Redis, S3, Qdrant and OpenAI:

    python worker.py --role all
    python worker.py --role page --threads 4 --image-threads 8
    python worker.py --role page          # second machine / terminal → more throughput

Reliability
  * at-least-once delivery: a message is ACKed only after its chunks are in Qdrant
  * crashed worker: its unacked pages are reclaimed by another worker (XAUTOCLAIM)
  * failing page: retried up to INGEST_MAX_ATTEMPTS, then moved to the dead-letter stream
  * idempotency: deterministic chunk IDs + exactly-once page counting (Lua script)
"""

import os
import sys
import time
import socket
import signal
import logging
import argparse
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fitz  # noqa: E402
import redis  # noqa: E402
from openai import OpenAI  # noqa: E402
from qdrant_client import QdrantClient  # noqa: E402

from ingest import config, blob  # noqa: E402
from ingest.bus import (  # noqa: E402
    get_redis, ensure_group, mark_page_done, claim_image, acquire_rate,
    publish_event, subscribe_events,
)
from ingest.jobs import TERMINAL_STATES, is_cancelled  # noqa: E402
import tools.extraction as extraction  # noqa: E402
import tools.ingestion as ingestion  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(threadName)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ingest-worker")

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
IMAGES_DIR = os.path.join(tempfile.gettempdir(), "dji_images")

# Release the per-document lock only if it still belongs to this job (compare-and-delete).
_RELEASE_LOCK_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""


class Worker:
    def __init__(self, roles: set, page_threads: int, image_threads: int):
        self.roles = roles
        self.page_threads = page_threads
        self.name = f"{socket.gethostname()}-{os.getpid()}"
        self.r = get_redis()
        self.stop = threading.Event()
        self.pages_processed = 0
        self.pages_lock = threading.Lock()
        self.current: dict[str, str] = {}

        # Concurrency inside one worker: images of a page are uploaded + captioned in parallel
        self.image_pool = ThreadPoolExecutor(max_workers=image_threads, thread_name_prefix="img")

        # OpenAI clients with more retries/back-off: many workers share one API key,
        # so 429s are expected under load and must be retried, not dropped.
        extraction.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"), max_retries=6)
        ingestion.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"), max_retries=6)

        self.qdrant = QdrantClient(url=QDRANT_URL)
        ingestion.ensure_collection(self.qdrant)
        self.release_lock = self.r.register_script(_RELEASE_LOCK_LUA)

        ensure_group(config.JOBS_STREAM, config.SPLITTER_GROUP)
        ensure_group(config.PAGES_STREAM, config.PAGE_GROUP)
        os.makedirs(IMAGES_DIR, exist_ok=True)

    # ── helpers ──────────────────────────────────────────────────────────────
    def job(self, job_id: str) -> dict:
        return self.r.hgetall(config.JOB_KEY.format(job_id=job_id))

    def rate_limited_caption(self, job_id: str):
        def _caption(img_bytes, page_num, img_index, manual_name):
            acquire_rate("caption", config.CAPTION_RPM)
            caption = extraction.caption_image(img_bytes, page_num, img_index, manual_name)
            if caption.startswith("[Image caption unavailable"):
                self.r.hincrby(config.JOB_KEY.format(job_id=job_id), "caption_failures", 1)
                logger.error(f"job={job_id} page={page_num} img={img_index} caption failed: {caption[:200]}")
            return caption
        return _caption

    # ── splitter ─────────────────────────────────────────────────────────────
    def handle_job_message(self, msg_id: str, fields: dict):
        job_id = fields.get("job_id")
        job = self.job(job_id)
        if not job or job.get("status") in TERMINAL_STATES or job.get("status") == "processing":
            self.r.xack(config.JOBS_STREAM, config.SPLITTER_GROUP, msg_id)
            return

        job_key = config.JOB_KEY.format(job_id=job_id)
        self.r.hset(job_key, mapping={"status": "splitting", "split_started_at": time.time(),
                                      "splitter": self.name})
        try:
            path = blob.local_pdf_path(job_id, job["blob_uri"])
            doc = fitz.open(path)
            page_indexes = [i for i in range(len(doc)) if extraction.page_has_content(doc, i)]
            doc_pages = len(doc)
            doc.close()
        except Exception as e:
            logger.error(f"job={job_id} split failed: {e}", exc_info=True)
            self.finish_job(job_id, status="failed", error=f"Could not open PDF: {e}")
            self.r.xack(config.JOBS_STREAM, config.SPLITTER_GROUP, msg_id)
            return

        if not page_indexes:
            self.finish_job(job_id, status="failed", error="No extractable text in PDF")
            self.r.xack(config.JOBS_STREAM, config.SPLITTER_GROUP, msg_id)
            return

        # total_pages MUST be written before the page tasks are published: otherwise a
        # fast worker could finish every page before the total is known.
        self.r.hset(job_key, mapping={"total_pages": len(page_indexes), "pdf_pages": doc_pages,
                                      "status": "processing", "split_done_at": time.time()})
        pipe = self.r.pipeline(transaction=False)
        for idx in page_indexes:
            pipe.xadd(config.PAGES_STREAM, {"job_id": job_id, "page_index": idx, "attempt": 0})
        pipe.xack(config.JOBS_STREAM, config.SPLITTER_GROUP, msg_id)
        pipe.execute()
        logger.info(f"job={job_id} split into {len(page_indexes)} page tasks "
                    f"({doc_pages - len(page_indexes)} empty pages skipped)")

    # ── page worker ──────────────────────────────────────────────────────────
    def process_page(self, job_id: str, job: dict, page_index: int) -> dict:
        t_page = time.perf_counter()
        stats: dict = {}
        path = blob.local_pdf_path(job_id, job["blob_uri"])
        doc = fitz.open(path)  # one Document per call: PyMuPDF objects are not thread-safe
        try:
            page = extraction.extract_single_page(
                doc, page_index, job["filename"], IMAGES_DIR,
                caption_images=job.get("caption_images", "1") == "1",
                upload_to_s3=True,
                claim_xref=lambda xref: claim_image(job_id, xref, page_index + 1),
                caption_fn=self.rate_limited_caption(job_id),
                executor=self.image_pool,
                stats=stats,
            )
        finally:
            doc.close()

        chunks = 0
        if page is not None:
            page["source"] = job["filename"]
            page["drone_model_override"] = job["drone_model"]
            _, children = ingestion.build_parent_child_chunks([page])

            if is_cancelled(job_id):
                logger.info(f"job={job_id} cancelled — page {page_index + 1} not written")
                return {"cancelled": True}

            for i in range(0, len(children), 64):
                batch = children[i:i + 64]
                t0 = time.perf_counter()
                acquire_rate("embed", config.EMBED_RPM)
                vectors = ingestion.get_embeddings([c["text"] for c in batch])
                t1 = time.perf_counter()
                self.qdrant.upsert(collection_name=ingestion.COLLECTION_NAME,
                                   points=ingestion.children_to_points(batch, vectors))
                t2 = time.perf_counter()
                stats["embed_seconds"] = stats.get("embed_seconds", 0.0) + (t1 - t0)
                stats["upsert_seconds"] = stats.get("upsert_seconds", 0.0) + (t2 - t1)
            chunks = len(children)

        stats["chunks"] = chunks
        stats["page_seconds"] = time.perf_counter() - t_page
        return stats

    def handle_page_message(self, msg_id: str, fields: dict, deliveries: int = 1):
        job_id = fields.get("job_id")
        page_index = int(fields.get("page_index", -1))
        attempt = int(fields.get("attempt", 0)) + deliveries  # 1-based attempt number
        job = self.job(job_id)

        if not job or job.get("status") in TERMINAL_STATES:
            self.r.xack(config.PAGES_STREAM, config.PAGE_GROUP, msg_id)
            return

        if attempt > config.MAX_ATTEMPTS:
            self.dead_letter(msg_id, fields, f"exceeded {config.MAX_ATTEMPTS} attempts (worker crashes/timeouts)")
            return

        self.current[threading.current_thread().name] = f"{job_id}:p{page_index + 1}"
        try:
            stats = self.process_page(job_id, job, page_index)
            if stats.get("cancelled"):
                self.r.xack(config.PAGES_STREAM, config.PAGE_GROUP, msg_id)
                return
            _, finished = mark_page_done(
                job_id, page_index + 1, self.name,
                chunks=stats.get("chunks", 0), images=stats.get("images", 0),
                caption_s=stats.get("caption_seconds", 0.0), embed_s=stats.get("embed_seconds", 0.0),
                upsert_s=stats.get("upsert_seconds", 0.0), s3_s=stats.get("s3_seconds", 0.0),
            )
            self.r.xack(config.PAGES_STREAM, config.PAGE_GROUP, msg_id)
            with self.pages_lock:
                self.pages_processed += 1
            total = int(self.job(job_id).get("total_pages", 0))
            logger.info(f"job={job_id} page {page_index + 1} done in {stats['page_seconds']:.1f}s "
                        f"({stats.get('images', 0)} img, {stats.get('chunks', 0)} chunks) "
                        f"[{finished}/{total}]")
            if total and finished >= total:
                self.finish_job(job_id)
        except Exception as e:
            logger.error(f"job={job_id} page {page_index + 1} attempt {attempt} failed: {e}", exc_info=True)
            if attempt >= config.MAX_ATTEMPTS:
                self.dead_letter(msg_id, fields, str(e))
            else:
                # immediate retry: re-publish with attempt+1, then ACK the failed delivery
                pipe = self.r.pipeline(transaction=True)
                pipe.xadd(config.PAGES_STREAM, {"job_id": job_id, "page_index": page_index, "attempt": attempt})
                pipe.xack(config.PAGES_STREAM, config.PAGE_GROUP, msg_id)
                pipe.execute()
        finally:
            self.current.pop(threading.current_thread().name, None)

    def dead_letter(self, msg_id: str, fields: dict, reason: str):
        job_id = fields.get("job_id")
        page_index = int(fields.get("page_index", -1))
        logger.error(f"job={job_id} page {page_index + 1} → dead-letter queue: {reason}")
        pipe = self.r.pipeline(transaction=True)
        pipe.xadd(config.DLQ_STREAM, {**fields, "reason": reason[:500], "worker": self.name,
                                      "failed_at": time.time()})
        pipe.xack(config.PAGES_STREAM, config.PAGE_GROUP, msg_id)
        pipe.execute()
        _, finished = mark_page_done(job_id, page_index + 1, self.name, failed=True)
        total = int(self.job(job_id).get("total_pages", 0))
        if total and finished >= total:
            self.finish_job(job_id)

    # ── job completion ───────────────────────────────────────────────────────
    def finish_job(self, job_id: str, status: str = None, error: str = None):
        job_key = config.JOB_KEY.format(job_id=job_id)
        job = self.job(job_id)
        if not job:
            return
        if job.get("status") == "cancelled":
            status = "cancelled"
        elif status is None:
            failed = int(job.get("pages_failed", 0))
            total = int(job.get("total_pages", 0))
            status = "failed" if total and failed >= total else (
                "completed_with_errors" if failed else "completed")

        mapping = {"status": status, "finished_at": time.time(), "finalized_by": self.name}
        if error:
            mapping["error"] = error
        self.r.hset(job_key, mapping=mapping)
        self.release_lock(keys=[config.ACTIVE_SOURCE_KEY.format(source=job.get("filename", ""))],
                          args=[job_id])
        blob.delete_pdf(job_id, job.get("blob_uri", ""))

        elapsed = time.time() - float(job.get("created_at", time.time()))
        event = "document.indexed" if status.startswith("completed") else f"job.{status}"
        publish_event(event, job_id=job_id, filename=job.get("filename"),
                      chunks=int(job.get("chunks", 0)), status=status)
        logger.info(f"job={job_id} {status.upper()} — '{job.get('filename')}' "
                    f"{job.get('chunks', 0)} chunks, {elapsed:.1f}s end-to-end")

    # ── loops ────────────────────────────────────────────────────────────────
    def consume(self, stream: str, group: str, consumer: str, handler):
        last_timeout_log = 0.0
        while not self.stop.is_set():
            try:
                resp = self.r.xreadgroup(group, consumer, {stream: ">"}, count=1, block=5000)
                for _, messages in resp or []:
                    for msg_id, fields in messages:
                        handler(msg_id, fields)
            except redis.TimeoutError:
                # A blocking read cut by the network (seen with Docker Desktop's port relay).
                # Benign: if a message was delivered in that instant it stays pending and is
                # reclaimed by XAUTOCLAIM. Retry at once; log at most once a minute.
                if time.time() - last_timeout_log > 60:
                    logger.warning(f"consumer {consumer}: blocking read timed out (network); retrying")
                    last_timeout_log = time.time()
            except Exception as e:
                if "NOGROUP" in str(e):
                    # Redis restarted without its data: recreate the stream + group and continue
                    logger.warning(f"consumer group '{group}' missing on '{stream}' — recreating")
                    try:
                        ensure_group(stream, group)
                    except Exception as e2:
                        logger.error(f"could not recreate group: {e2}")
                else:
                    logger.error(f"consumer {consumer} error: {e}", exc_info=True)
                time.sleep(2)

    def reclaim_loop(self):
        """Take over messages left unacknowledged by crashed/stuck workers."""
        consumer = f"{self.name}-reclaimer"
        targets = []
        if "page" in self.roles:
            targets.append((config.PAGES_STREAM, config.PAGE_GROUP, self.handle_page_message))
        if "splitter" in self.roles:
            targets.append((config.JOBS_STREAM, config.SPLITTER_GROUP,
                            lambda m, f, d: self.handle_job_message(m, f)))
        while not self.stop.wait(config.RECLAIM_INTERVAL_S):
            for stream, group, handler in targets:
                try:
                    res = self.r.xautoclaim(stream, group, consumer,
                                            min_idle_time=config.RECLAIM_IDLE_MS, start_id="0-0", count=10)
                    messages = res[1] if len(res) > 1 else []
                    for msg_id, fields in messages:
                        if not fields:
                            continue
                        info = self.r.xpending_range(stream, group, min=msg_id, max=msg_id, count=1)
                        deliveries = info[0]["times_delivered"] if info else 1
                        logger.warning(f"reclaimed {stream} message {msg_id} (delivery #{deliveries}) "
                                       f"from a crashed or stuck worker")
                        handler(msg_id, fields, deliveries)
                except Exception as e:
                    logger.error(f"reclaim loop error on {stream}: {e}", exc_info=True)

    def heartbeat_loop(self):
        key = config.WORKER_HEARTBEAT_KEY.format(name=self.name)
        while not self.stop.is_set():
            try:
                self.r.hset(key, mapping={
                    "name": self.name, "roles": ",".join(sorted(self.roles)),
                    "threads": self.page_threads, "pages_processed": self.pages_processed,
                    "busy": ";".join(self.current.values()), "last_seen": time.time(),
                })
                self.r.expire(key, config.HEARTBEAT_TTL_S)
            except Exception as e:
                logger.warning(f"heartbeat failed: {e}")
            self.stop.wait(10)
        self.r.delete(key)

    def on_event(self, event: dict):
        # every worker drops its cached copy of a finished document
        if event.get("type") in ("document.indexed", "job.failed", "job.cancelled"):
            blob.drop_local_cache(event.get("job_id", ""))

    def run(self):
        threads = [threading.Thread(target=self.heartbeat_loop, name="heartbeat", daemon=True),
                   threading.Thread(target=self.reclaim_loop, name="reclaim", daemon=True)]
        if "splitter" in self.roles:
            threads.append(threading.Thread(
                target=self.consume, name="splitter", daemon=True,
                args=(config.JOBS_STREAM, config.SPLITTER_GROUP, f"{self.name}-splitter",
                      self.handle_job_message)))
        if "page" in self.roles:
            for i in range(self.page_threads):
                threads.append(threading.Thread(
                    target=self.consume, name=f"page-{i}", daemon=True,
                    args=(config.PAGES_STREAM, config.PAGE_GROUP, f"{self.name}-t{i}",
                          lambda m, f: self.handle_page_message(m, f, 1))))
        subscribe_events(self.on_event, name=self.name)
        for t in threads:
            t.start()
        logger.info(f"Worker {self.name} started — roles={sorted(self.roles)} "
                    f"page_threads={self.page_threads} image_threads={self.image_pool._max_workers} "
                    f"caption_rpm={config.CAPTION_RPM}")
        try:
            while not self.stop.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        logger.info("Shutting down…")
        self.stop.set()
        self.image_pool.shutdown(wait=False, cancel_futures=True)


def main():
    parser = argparse.ArgumentParser(description="Distributed ingestion worker")
    parser.add_argument("--role", choices=["splitter", "page", "all"], default="all")
    parser.add_argument("--threads", type=int, default=config.DEFAULT_PAGE_THREADS,
                        help="pages processed concurrently by this worker")
    parser.add_argument("--image-threads", type=int, default=config.DEFAULT_IMAGE_THREADS,
                        help="images captioned concurrently by this worker")
    args = parser.parse_args()
    roles = {"splitter", "page"} if args.role == "all" else {args.role}
    worker = Worker(roles, args.threads, args.image_threads)
    signal.signal(signal.SIGTERM, lambda *_: worker.stop.set())
    worker.run()


if __name__ == "__main__":
    main()

"""
Redis coordination layer: streams, consumer groups, atomic job counters,
distributed rate limiting and event publishing.

Every operation that several workers may run at the same time is atomic
(single Redis command, MULTI/EXEC transaction or Lua script), so the pipeline
stays correct with any number of worker processes on any number of machines.
"""

import json
import time
import logging
import threading

import redis
from redis.backoff import ExponentialBackoff
from redis.retry import Retry

from ingest import config

logger = logging.getLogger(__name__)

_client = None
_lock = threading.Lock()


def get_redis() -> redis.Redis:
    """Process-wide Redis client (redis-py clients are thread-safe connection pools)."""
    global _client
    if _client is None:
        with _lock:
            if _client is None:
                # Survive Redis restarts: stale pooled sockets are detected by the health
                # check, and connection/timeout errors are retried with exponential back-off.
                _client = redis.from_url(
                    config.REDIS_URL,
                    decode_responses=True,
                    health_check_interval=15,
                    socket_keepalive=True,
                    socket_timeout=30,          # > the 5 s XREADGROUP block
                    socket_connect_timeout=5,
                    retry=Retry(ExponentialBackoff(cap=5, base=0.2), retries=5),
                    retry_on_error=[redis.ConnectionError, redis.TimeoutError],
                )
                _client.ping()
    return _client


def ensure_group(stream: str, group: str):
    """Create the consumer group (and the stream) if it does not exist yet."""
    try:
        get_redis().xgroup_create(stream, group, id="0", mkstream=True)
        logger.info(f"Created consumer group '{group}' on '{stream}'")
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


# ── Atomic "page finished" ───────────────────────────────────────────────────
# Marks a page as finished exactly once, even if the same page is processed twice
# (at-least-once delivery: a worker can crash after upserting but before XACK).
# Counters are only incremented the FIRST time the page is added to the set, and
# the script returns how many pages are finished so that exactly one worker — the
# one that brings the count to total_pages — finalizes the job.
_MARK_PAGE_DONE_LUA = """
local added = redis.call('SADD', KEYS[2], ARGV[1])
if added == 1 then
  redis.call('HINCRBY', KEYS[1], 'chunks', ARGV[2])
  redis.call('HINCRBY', KEYS[1], 'images', ARGV[3])
  redis.call('HINCRBYFLOAT', KEYS[1], 'caption_seconds', ARGV[4])
  redis.call('HINCRBYFLOAT', KEYS[1], 'embed_seconds', ARGV[5])
  redis.call('HINCRBYFLOAT', KEYS[1], 'upsert_seconds', ARGV[6])
  redis.call('HINCRBYFLOAT', KEYS[1], 's3_seconds', ARGV[7])
  if ARGV[8] == '1' then
    redis.call('HINCRBY', KEYS[1], 'pages_failed', 1)
  else
    redis.call('HINCRBY', KEYS[1], 'pages_succeeded', 1)
  end
  redis.call('HINCRBY', KEYS[3], ARGV[9], 1)
end
local finished = redis.call('SCARD', KEYS[2])
redis.call('HSET', KEYS[1], 'pages_finished', finished)
return {added, finished}
"""
_mark_page_done_script = None


def mark_page_done(job_id: str, page: int, worker: str, chunks: int = 0, images: int = 0,
                   caption_s: float = 0.0, embed_s: float = 0.0, upsert_s: float = 0.0,
                   s3_s: float = 0.0, failed: bool = False) -> tuple[bool, int]:
    """Returns (first_time, pages_finished)."""
    global _mark_page_done_script
    r = get_redis()
    if _mark_page_done_script is None:
        _mark_page_done_script = r.register_script(_MARK_PAGE_DONE_LUA)
    added, finished = _mark_page_done_script(
        keys=[config.JOB_KEY.format(job_id=job_id),
              config.JOB_DONE_PAGES_KEY.format(job_id=job_id),
              config.JOB_WORKERS_KEY.format(job_id=job_id)],
        args=[page, chunks, images, round(caption_s, 4), round(embed_s, 4),
              round(upsert_s, 4), round(s3_s, 4), "1" if failed else "0", worker],
    )
    return bool(added), int(finished)


# ── Image de-duplication across pages/workers ────────────────────────────────
def claim_image(job_id: str, xref: int, page: int) -> bool:
    """
    An image (PDF xref) shared by several pages must be captioned only once.
    SET NX gives ownership to the first page that claims it. A retried page
    still owns the images it claimed on its first attempt (value == page).
    """
    r = get_redis()
    key = config.JOB_IMAGE_CLAIM_KEY.format(job_id=job_id, xref=xref)
    if r.set(key, page, nx=True, ex=config.JOB_TTL_S):
        return True
    return r.get(key) == str(page)


# ── Distributed rate limiter (fixed window, shared by all workers) ───────────
def acquire_rate(bucket: str, limit_per_minute: int):
    """
    Block until a slot is available in this minute's window for `bucket`.
    SET NX EX + INCR run in one MULTI/EXEC transaction, so the window key always
    has an expiry (no permanent lock-out if a process dies between commands).
    """
    r = get_redis()
    while True:
        window = int(time.time() // 60)
        key = f"ingest:ratelimit:{bucket}:{window}"
        pipe = r.pipeline(transaction=True)
        pipe.set(key, 0, ex=65, nx=True)
        pipe.incr(key)
        _, count = pipe.execute()
        if count <= limit_per_minute:
            return
        # window full: wait for the next minute
        time.sleep(max(0.05, 60 - (time.time() % 60)))


# ── Events ───────────────────────────────────────────────────────────────────
def publish_event(event_type: str, **payload):
    get_redis().publish(config.EVENTS_CHANNEL, json.dumps({"type": event_type, **payload}))


def subscribe_events(handler, name: str = "listener") -> threading.Thread:
    """
    Run `handler(event_dict)` for every event on the ingest channel, in a daemon
    thread. Reconnects automatically if Redis restarts.
    """
    def _loop():
        while True:
            try:
                pubsub = redis.from_url(config.REDIS_URL, decode_responses=True).pubsub(
                    ignore_subscribe_messages=True)
                pubsub.subscribe(config.EVENTS_CHANNEL)
                logger.info(f"[{name}] subscribed to '{config.EVENTS_CHANNEL}'")
                for msg in pubsub.listen():
                    if msg.get("type") != "message":
                        continue
                    try:
                        handler(json.loads(msg["data"]))
                    except Exception as e:
                        logger.error(f"[{name}] event handler error: {e}", exc_info=True)
            except Exception as e:
                logger.warning(f"[{name}] event subscription lost ({e}); retrying in 5s")
                time.sleep(5)

    t = threading.Thread(target=_loop, name=f"ingest-events-{name}", daemon=True)
    t.start()
    return t

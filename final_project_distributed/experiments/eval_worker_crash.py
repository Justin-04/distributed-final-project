"""
Evaluation — an ingestion worker crashes while it is processing pages (Docker Compose stack).

    python eval_worker_crash.py <pdf>

1. uploads the PDF through the gateway (temporary document name, deleted at the end)
2. after a few seconds, shows the pending list of the `page-workers` consumer group
   (pages handed to a worker but not yet acknowledged with XACK)
3. kills the worker container holding the most unacknowledged pages (docker kill = crash)
4. follows those pages: they stay pending under the dead consumer; after RECLAIM_IDLE
   (120 s) another worker takes them over with XAUTOCLAIM (delivery count 2)
5. waits for the job to finish and checks the result (pages, failed pages, chunks)

Results are appended to experiments/results/eval_worker_crash.jsonl.
"""

import os
import sys
import json
import time
import base64
import random
import subprocess
import urllib.request
import urllib.error
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

API = "http://127.0.0.1:8080"                     # the gateway
REDIS = "redis-cache"                             # Redis container of the Compose stack
STREAM, GROUP = "ingest:pages", "page-workers"
RESULTS = Path(__file__).parent / "results" / "eval_worker_crash.jsonl"
T0 = time.time()


def log(msg):
    print(f"[t={time.time() - T0:6.1f}s] {msg}", flush=True)


def call(path, body=None, token=None, method=None, timeout=300):
    h = {"Content-Type": "application/json"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(API + path, data=json.dumps(body).encode() if body is not None else None,
                                 headers=h, method=method or ("POST" if body is not None else "GET"))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, None


def redis(*args):
    return subprocess.run(["docker", "exec", REDIS, "redis-cli", *args],
                          capture_output=True, text=True).stdout.strip()


def pending_by_consumer():
    """{consumer: count} from XPENDING summary."""
    out = redis("XPENDING", STREAM, GROUP).splitlines()
    # summary: count, min id, max id, then consumer/count pairs
    return {out[i]: int(out[i + 1]) for i in range(3, len(out) - 1, 2)} if len(out) > 3 else {}


def pending_entries():
    """[(id, consumer, idle_ms, deliveries)] from the extended XPENDING form."""
    out = redis("XPENDING", STREAM, GROUP, "-", "+", "100").splitlines()
    return [(out[i], out[i + 1], int(out[i + 2]), int(out[i + 3])) for i in range(0, len(out) - 3, 4)]


def container_of(consumer):
    """Consumer names start with the container hostname (= short container ID)."""
    cid = consumer.split("-")[0]
    return subprocess.run(["docker", "ps", "-a", "--filter", f"id={cid}", "--format", "{{.Names}}"],
                          capture_output=True, text=True).stdout.strip()


def main(pdf):
    admin = f"eval_admin_{random.randint(10000, 99999)}"
    s, d = call("/api/v1/auth/register", {"username": admin, "password": "Test12345!", "role": "admin"})
    tok = d["access_token"]
    name = f"EVAL_CRASH_{os.path.basename(pdf)}"
    with open(pdf, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    s, d = call("/api/v1/admin/ingest", {"pdf_file": b64, "drone_model": "DJI Neo", "source_name": name}, tok)
    job = d["job_id"]
    log(f"upload accepted: HTTP {s}, job {job}")

    time.sleep(10)
    for _ in range(60):  # wait until the workers are busy with pages
        pend = pending_by_consumer()
        if sum(pend.values()) >= 3:
            break
        s, j = call(f"/api/v1/admin/ingest/jobs/{job}", token=tok)
        if j and j["status"] == "failed":
            log(f"job failed before any page was processed: {j.get('error')}")
            return
        time.sleep(1)
    log(f"pages being processed right now (unacknowledged), per worker: {pend}")
    victim_consumer = max(pend, key=pend.get)
    victim = container_of(victim_consumer)
    lost = [e for e in pending_entries() if e[1] == victim_consumer]
    log(f"FAILURE: docker kill {victim}  (consumer {victim_consumer}, {len(lost)} page(s) in progress)")
    subprocess.run(["docker", "kill", victim], capture_output=True)
    t_kill = time.time()
    lost_ids = {e[0] for e in lost}

    reported, reclaimed = set(), {}
    while True:
        entries = {e[0]: e for e in pending_entries()}
        for mid in lost_ids - reported:
            e = entries.get(mid)
            if e and e[1] != victim_consumer:
                reclaimed[mid] = e
                reported.add(mid)
                log(f"RECLAIMED {mid}: now owned by {e[1]}, delivery #{e[3]} "
                    f"({time.time() - t_kill:.0f}s after the crash)")
            elif mid not in entries:
                reported.add(mid)
                log(f"page message {mid}: acknowledged (finished by the worker that reclaimed it)")
        if int(time.time() - t_kill) % 20 == 0:
            still = [m for m in lost_ids if m in entries and entries[m][1] == victim_consumer]
            if still:
                log(f"{len(still)} page(s) still pending under the dead worker, idle "
                    f"{max(entries[m][2] for m in still) // 1000}s (reclaimed after 120s idle)")
        s, j = call(f"/api/v1/admin/ingest/jobs/{job}", token=tok)
        if j and j["status"] in ("completed", "completed_with_errors", "failed", "cancelled"):
            break
        time.sleep(1)

    log(f"JOB {j['status']}: {j['pages_finished']}/{j['total_pages']} pages, {j['pages_failed']} failed, "
        f"{j['chunks']} chunks, {j['timings']['elapsed_seconds']}s; pages per worker {j['workers']}")
    res = {"killed": victim, "pages_in_progress_at_crash": len(lost), "reclaimed": len(reclaimed),
           "status": j["status"], "pages": j["total_pages"], "failed": j["pages_failed"],
           "chunks": j["chunks"], "elapsed_s": j["timings"]["elapsed_seconds"],
           "ts": time.strftime("%Y-%m-%d %H:%M:%S")}
    RESULTS.parent.mkdir(exist_ok=True)
    with open(RESULTS, "a", encoding="utf-8") as f:
        f.write(json.dumps(res) + "\n")
    s, _ = call(f"/api/v1/admin/documents/{urllib.request.quote(name)}", token=tok, method="DELETE")
    log(f"cleanup: temporary document deleted (HTTP {s}); restart the worker with: docker start {victim}")


if __name__ == "__main__":
    main(sys.argv[1])

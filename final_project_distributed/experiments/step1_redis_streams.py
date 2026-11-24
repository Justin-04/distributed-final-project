"""
Step 1 — Redis Streams ingestion pipeline: measurements, real bugs and fault injection.

    python experiments/step1_redis_streams.py upload <pdf>        # 202 time, progress, per-worker pages
    python experiments/step1_redis_streams.py crash <pdf>         # kill a worker mid-job (fault injection)
    python experiments/step1_redis_streams.py duplicate <pdf>     # same PDF twice at once (fault injection)
    python experiments/step1_redis_streams.py redelivery          # same page processed twice (at-least-once)
    python experiments/step1_redis_streams.py redis-restart       # restart Redis while the system runs

Local setup: Agent A 127.0.0.1:8000, MCP server 127.0.0.1:8002, workers started with
`python worker.py --role all|page` from services/mcp-server, Redis on :6380 (RedisInsight :8001).
Uploads use a temporary document name and are deleted afterwards.
Results are appended to experiments/results/step1_redis_streams.jsonl.
"""

import os
import sys
import json
import time
import base64
import random
import subprocess
import threading
import urllib.request
import urllib.error
from datetime import datetime

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

API = "http://127.0.0.1:8000"
MCP = "http://127.0.0.1:8002"
QDRANT = "http://127.0.0.1:6333/collections/dji_manuals_parent_child"
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", "step1_redis_streams.jsonl")
T0 = time.time()


def log(msg):
    print(f"[t+{time.time() - T0:6.1f}s] {msg}", flush=True)


def call(base, path, body=None, token=None, method=None, timeout=600):
    h = {"Content-Type": "application/json"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(base + path, method=method or ("POST" if body is not None else "GET"),
                                 data=json.dumps(body).encode() if body is not None else None, headers=h)
    t = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}"), time.perf_counter() - t
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read() or b"{}")
        except Exception:
            body = None
        return e.code, body, time.perf_counter() - t
    except Exception as e:
        return 0, {"error": f"{type(e).__name__}: {e}"}, time.perf_counter() - t


def save(test, **data):
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "a", encoding="utf-8") as f:
        f.write(json.dumps({"time": datetime.now().isoformat(timespec="seconds"), "test": test, **data}) + "\n")


def admin():
    name = f"step1_admin_{random.randint(10000, 99999)}"
    s, d, _ = call(API, "/api/v1/auth/register", {"username": name, "password": "Test12345!", "role": "admin"})
    assert s == 200, f"register failed: {s}"
    return d["access_token"]


def submit(tok, pdf, name):
    with open(pdf, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    return call(API, "/api/v1/admin/ingest", {"pdf_file": b64, "drone_model": "DJI Neo", "source_name": name}, tok)


def wait_job(tok, job_id, quiet=False):
    last = None
    while True:
        s, j, _ = call(API, f"/api/v1/admin/ingest/jobs/{job_id}", token=tok)
        if s != 200:
            log(f"status request failed: HTTP {s}")
            time.sleep(2)
            continue
        line = f"{j['status']:<22} {j['pages_finished']}/{j['total_pages']} pages, {len(j['workers'])} worker(s)"
        if line != last and not quiet:
            log(line)
            last = line
        if j["status"] in ("completed", "completed_with_errors", "failed", "cancelled"):
            return j
        time.sleep(1.5)


def delete(tok, name):
    s, d, _ = call(API, f"/api/v1/admin/documents/{urllib.request.quote(name)}", token=tok, method="DELETE")
    log(f"cleanup '{name}': HTTP {s} {(d or {}).get('message', '')}")


def qdrant_count(source):
    body = {"exact": True, "filter": {"must": [{"key": "source", "match": {"value": source}}]}}
    req = urllib.request.Request(QDRANT + "/points/count", data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    return json.loads(urllib.request.urlopen(req, timeout=30).read())["result"]["count"]


def kill_one_page_worker():
    ps = ("$w = Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | Where-Object { $_.CommandLine -match "
          "'worker.py --role page' } | Sort-Object ProcessId | Select-Object -Last 1; if ($w) { "
          "Stop-Process -Id $w.ProcessId -Force; Stop-Process -Id $w.ParentProcessId -Force -ErrorAction SilentlyContinue; "
          "\"killed page worker pid $($w.ProcessId)\" } else { 'no page worker found' }")
    return subprocess.run(["powershell", "-NoProfile", "-Command", ps], capture_output=True, text=True).stdout.strip()


# ── scenarios ────────────────────────────────────────────────────────────────
def upload(pdf):
    tok = admin()
    name = f"STEP1_{os.path.basename(pdf)}"
    log(f"uploading {name} ({os.path.getsize(pdf) // 1048576} MB)…")
    s, d, dt = submit(tok, pdf, name)
    log(f"upload answered: HTTP {s} in {dt:.1f}s → job {(d or {}).get('job_id')}  (Step 0: request blocked 139.3 s)")
    if s != 202:
        log(f"unexpected answer: {d}")
        return
    j = wait_job(tok, d["job_id"])
    t = j["timings"]
    log(f"DONE: {j['chunks']} chunks, {j['images']} images, {t['elapsed_seconds']}s end-to-end; pages per worker {j['workers']}")
    save("upload", pdf=os.path.basename(pdf), accept_seconds=round(dt, 1), status=j["status"],
         elapsed_seconds=t["elapsed_seconds"], pages=j["total_pages"], chunks=j["chunks"],
         workers=j["workers"], failed_pages=j["pages_failed"])
    delete(tok, name)


def crash(pdf):
    tok = admin()
    name = f"STEP1_CRASH_{os.path.basename(pdf)}"
    s, d, dt = submit(tok, pdf, name)
    log(f"upload: HTTP {s} in {dt:.1f}s, job {d['job_id']}")
    time.sleep(12)
    log(f"FAULT INJECTION: {kill_one_page_worker()}")
    log("its in-progress pages stay pending in Redis; another worker reclaims them after 120 s (XAUTOCLAIM)…")
    j = wait_job(tok, d["job_id"])
    log(f"job {j['status']}: {j['pages_finished']}/{j['total_pages']} pages, {j['pages_failed']} failed, "
        f"{j['timings']['elapsed_seconds']}s; pages per worker {j['workers']}")
    save("worker_crash", status=j["status"], pages=j["total_pages"], failed=j["pages_failed"],
         elapsed_seconds=j["timings"]["elapsed_seconds"], workers=j["workers"])
    delete(tok, name)
    log("restart the killed worker:  python worker.py --role page   (from services/mcp-server)")


def duplicate(pdf):
    tok = admin()
    name = f"STEP1_DUP_{os.path.basename(pdf)}"
    res = [None, None]

    def go(i):
        res[i] = submit(tok, pdf, name)

    ts = [threading.Thread(target=go, args=(i,)) for i in range(2)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    for i, (s, d, dt) in enumerate(res):
        log(f"upload {i + 1}: HTTP {s} in {dt:.1f}s {(d or {}).get('job_id') or (d or {}).get('detail', '')}")
    save("duplicate_upload", statuses=[r[0] for r in res])
    job = next((r[1]["job_id"] for r in res if r[0] == 202), None)
    if job:
        wait_job(tok, job, quiet=True)
    delete(tok, name)


def redelivery():
    """Process the same page twice (what happens when a worker crashes after writing but before XACK)."""
    import redis
    tok = admin()
    pdf = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "DJI_Neo_User_Manual_v1.0_en.pdf")
    name = "STEP1_REDELIVERY_DJI_Neo.pdf"
    s, d, _ = submit(tok, pdf, name)
    j = wait_job(tok, d["job_id"], quiet=True)
    before = qdrant_count(name)
    log(f"job done: {before} chunks in Qdrant for '{name}'")
    r = redis.from_url("redis://localhost:6380", decode_responses=True)
    r.hset(f"ingest:job:{d['job_id']}", "status", "processing")  # job open again for the duplicate delivery
    r.xadd("ingest:pages", {"job_id": d["job_id"], "page_index": 9, "attempt": 0})
    log("FAULT INJECTION: page 10 delivered a second time (XADD of the same task)")
    time.sleep(25)
    after = qdrant_count(name)
    job = r.hgetall(f"ingest:job:{d['job_id']}")
    log(f"after re-processing page 10: {after} chunks (before {before}); pages_finished={job.get('pages_finished')}/{job.get('total_pages')}")
    save("redelivery", chunks_before=before, chunks_after=after,
         pages_finished=job.get("pages_finished"), total_pages=job.get("total_pages"))
    r.hset(f"ingest:job:{d['job_id']}", "status", "completed")
    delete(tok, name)


def redis_restart():
    log(f"before: workers endpoint → HTTP {call(MCP, '/api/v1/ingest/workers')[0]}")
    log("FAULT INJECTION: docker restart redis-stack")
    subprocess.run(["docker", "restart", "redis-stack"], capture_output=True)
    time.sleep(3)
    codes = []
    for i in range(5):
        s, d, dt = call(MCP, "/api/v1/ingest/workers", timeout=30)
        codes.append(s)
        log(f"  request {i + 1} after restart: HTTP {s} in {dt:.2f}s {'' if s == 200 else str(d)[:120]}")
        time.sleep(2)
    save("redis_restart", statuses=codes)
    log("now check the worker terminals / logs for Redis errors")


if __name__ == "__main__":
    cmd = sys.argv[1]
    arg = sys.argv[2] if len(sys.argv) > 2 else None
    {"upload": lambda: upload(arg), "crash": lambda: crash(arg), "duplicate": lambda: duplicate(arg),
     "redelivery": redelivery, "redis-restart": redis_restart}[cmd]()
    print(f"results appended to {OUT}")

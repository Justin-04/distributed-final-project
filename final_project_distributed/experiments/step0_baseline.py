"""
Step 0 — baseline measurements of the base system (before any distributed change).

    python experiments/step0_baseline.py chat          # single chat + 3 parallel chats
    python experiments/step0_baseline.py ingest <pdf>  # blocking upload + chat during ingestion
    python experiments/step0_baseline.py outage        # Agent A stopped: what users see

Local setup: Agent A on 127.0.0.1:8000 (one instance), frontend (Vite) on localhost:5173.
The ingest test uploads the PDF under a temporary name and deletes it afterwards.
Results are printed and appended to experiments/results/step0_baseline.jsonl.
"""

import os
import sys
import json
import time
import base64
import random
import threading
import urllib.request
import urllib.error
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

API = os.getenv("AGENT_A_URL", "http://127.0.0.1:8000")
SITE = os.getenv("SITE_URL", "http://localhost:5173")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", "step0_baseline.jsonl")


def call(base, path, body=None, token=None, method=None, timeout=900):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(base + path, method=method or ("POST" if body is not None else "GET"),
                                 data=json.dumps(body).encode() if body is not None else None, headers=headers)
    t = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}"), time.perf_counter() - t
    except urllib.error.HTTPError as e:
        return e.code, None, time.perf_counter() - t
    except Exception as e:
        return 0, {"error": f"{type(e).__name__}: {e}"}, time.perf_counter() - t


def user(role="user"):
    name = f"step0_{role}_{random.randint(10000, 99999)}"
    s, d, _ = call(API, "/api/v1/auth/register", {"username": name, "password": "Test12345!", "role": role})
    assert s == 200, f"register failed: {s}"
    return name, d["access_token"]


def save(test, **data):
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "a", encoding="utf-8") as f:
        f.write(json.dumps({"time": datetime.now().isoformat(timespec="seconds"), "test": test, **data}) + "\n")


def chat(name, token, q):
    s, d, dt = call(API, "/api/v1/chat", {"query": q, "user_id": name}, token)
    return s, dt, ((d or {}).get("metadata") or {}).get("route"), ((d or {}).get("response") or "")[:90].replace("\n", " ")


def test_chat():
    name, tok = user()
    print("warm-up question (loads the reranker / BM25 on first search)…")
    s, dt, route, ans = chat(name, tok, "What is the weight of the DJI Mini 4 Pro?")
    print(f"  warm-up: HTTP {s} in {dt:.1f}s")

    s, dt, route, ans = chat(name, tok, "What is the maximum flight time of the DJI Air 3?")
    print(f"[single chat] HTTP {s} in {dt:.1f}s route={route} | {ans}")
    save("single_chat", status=s, seconds=round(dt, 1), route=route)

    qs = ["What is the max wind resistance of the DJI Mini 4 Pro?",
          "How do I calibrate the compass of the DJI Air 3?",
          "What is the takeoff weight of the DJI Neo?"]
    t0 = time.perf_counter()
    with ThreadPoolExecutor(3) as ex:
        res = list(ex.map(lambda q: chat(name, tok, q), qs))
    wall = time.perf_counter() - t0
    lats = [round(r[1], 1) for r in res]
    print(f"[3 parallel chats, ONE Agent A] wall {wall:.1f}s, latencies {lats} "
          f"(single question ≈ {dt:.1f}s → parallel would be ≈ {dt:.1f}s if requests ran concurrently)")
    save("parallel_chats", n=3, wall_seconds=round(wall, 1), latencies=lats, single_seconds=round(dt, 1))


def test_ingest(pdf):
    admin, tok = user("admin")
    name = f"BASELINE_{os.path.basename(pdf)}"
    with open(pdf, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    chatter, ctok = user()
    result = {}

    def upload():
        s, d, dt = call(API, "/api/v1/admin/ingest",
                        {"pdf_file": b64, "drone_model": "DJI Neo", "source_name": name}, tok)
        result.update(status=s, seconds=dt, body=d)

    print(f"uploading {name} ({os.path.getsize(pdf) // 1048576} MB) through the admin API…")
    th = threading.Thread(target=upload)
    t0 = time.perf_counter()
    th.start()
    time.sleep(20)  # let ingestion get going, then a normal user asks a question
    s, dt, route, ans = chat(chatter, ctok, "What does error code E001 mean?")
    print(f"[chat DURING ingestion] HTTP {s} in {dt:.1f}s route={route} | {ans}")
    th.join()
    up = time.perf_counter() - t0
    body = result.get("body") or {}
    print(f"[blocking upload] HTTP {result.get('status')} after {result['seconds']:.1f}s "
          f"(the admin's request stayed open the whole time) → {body.get('pages')} pages, {body.get('chunks')} chunks")
    save("blocking_ingest", pdf=os.path.basename(pdf), status=result.get("status"),
         seconds=round(result["seconds"], 1), pages=body.get("pages"), chunks=body.get("chunks"),
         chat_during_ingest_seconds=round(dt, 1), chat_during_ingest_status=s)

    s, d, _ = call(API, f"/api/v1/admin/documents/{urllib.request.quote(name)}", token=tok, method="DELETE")
    print(f"cleanup: HTTP {s} {(d or {}).get('message', '')}")


def test_outage():
    print("Stop Agent A now (Ctrl+C in its terminal, or kill the process on port 8000),")
    print("then press Enter to send requests through the website…")
    input()
    codes = []
    for i in range(5):
        s, _, dt = call(SITE, "/api/v1/auth/login", {"username": "x", "password": "y"}, timeout=15)
        codes.append(s)
        print(f"  request {i + 1}: HTTP {s or 'connection failed'} in {dt:.1f}s")
        time.sleep(1)
    failed = sum(1 for c in codes if c not in (200, 401))
    print(f"[Agent A down] {failed}/5 requests failed — no other instance can take over")
    save("agent_a_outage", requests=5, failed=failed, statuses=codes)


if __name__ == "__main__":
    what = sys.argv[1] if len(sys.argv) > 1 else "chat"
    if what == "chat":
        test_chat()
    elif what == "ingest":
        test_ingest(sys.argv[2])
    elif what == "outage":
        test_outage()
    print(f"results appended to {OUT}")

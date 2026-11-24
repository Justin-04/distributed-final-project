"""
Step 3 experiments: what monitoring revealed.

    python step3_monitoring.py freeze [--chats 3] [--port 8000]
        Sends new (uncached) chat questions to ONE replica while probing that replica's
        /health every 0.2 s. A healthy async server answers /health in a few ms even
        while a chat is running; a long probe means the replica's event loop was frozen.

Results are appended to experiments/results/step3_monitoring.jsonl.
"""

import sys
import json
import time
import uuid
import random
import argparse
import threading
import urllib.request
from pathlib import Path

RESULTS = Path(__file__).parent / "results" / "step3_monitoring.jsonl"

QUESTIONS = [
    "How long does the DJI Mini 4 Pro battery take to charge with the 30W charger?",
    "What does the DJI Air 3 do when the remote controller signal is lost?",
    "How do I update the firmware of the DJI Avata 2 goggles?",
    "What is the maximum wind resistance of the DJI Mini 4 Pro?",
    "How do I enable obstacle avoidance on the DJI Air 3?",
    "How should I store DJI Intelligent Flight Batteries for a long time?",
]


def call(url, method="GET", body=None, token=None, timeout=300):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, time.perf_counter() - t0, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, time.perf_counter() - t0, None
    except Exception as e:
        return 0, time.perf_counter() - t0, str(e)


def register(base):
    user = f"s3_{random.randint(10000, 99999)}"
    s, _, d = call(f"{base}/api/v1/auth/register", "POST",
                   {"username": user, "password": "Test12345!", "role": "user"})
    return user, d["access_token"]


def freeze(port, chats):
    base = f"http://127.0.0.1:{port}"
    user, token = register(base)
    probes, stop = [], threading.Event()

    def probe():
        while not stop.is_set():
            t = time.perf_counter()
            s, dt, _ = call(f"{base}/health", timeout=60)
            probes.append((t, dt, s))
            time.sleep(0.2)

    th = threading.Thread(target=probe, daemon=True)
    th.start()
    chat_times = []
    for q in random.sample(QUESTIONS, chats):
        q = f"{q} (ref {uuid.uuid4().hex[:6]})"          # never a cache hit
        s, dt, d = call(f"{base}/api/v1/chat", "POST", {"query": q, "user_id": user}, token)
        chat_times.append(round(dt, 1))
        print(f"[freeze] chat {s} in {dt:.1f}s")
    time.sleep(12)  # the trace upload happens after the answer: keep probing
    stop.set()
    th.join()

    lat = [dt for _, dt, _ in probes]
    slow = [round(dt, 1) for dt in lat if dt > 1.0]
    res = {"test": "freeze", "port": port, "chats": chat_times, "probes": len(lat),
           "health_max_s": round(max(lat), 2),
           "health_median_ms": round(sorted(lat)[len(lat) // 2] * 1000, 1),
           "probes_over_1s": len(slow), "slow_probes_s": slow,
           "probe_failures": sum(1 for _, _, s in probes if s != 200)}
    print(f"[freeze] /health on :{port}: {len(lat)} probes, median "
          f"{res['health_median_ms']} ms, max {res['health_max_s']} s, "
          f"{len(slow)} probe(s) over 1 s: {slow}")
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("test", choices=["freeze"])
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--chats", type=int, default=3)
    ap.add_argument("--label", default="")
    a = ap.parse_args()
    res = freeze(a.port, a.chats)
    res["label"] = a.label
    res["ts"] = time.strftime("%Y-%m-%d %H:%M:%S")
    RESULTS.parent.mkdir(exist_ok=True)
    with open(RESULTS, "a", encoding="utf-8") as f:
        f.write(json.dumps(res) + "\n")

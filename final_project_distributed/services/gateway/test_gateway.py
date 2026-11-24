"""
API gateway tests: load distribution, overhead, failover, edge rate limiting.

    python test_gateway.py all                 # every test below (kills one replica!)
    python test_gateway.py distribution overhead chat ratelimit
    python test_gateway.py failover --kill-port 8003
    python test_gateway.py failover --kill-container agent-a-2   # replicas in Docker

Assumes the gateway on :8080 and Agent A replicas on 8000, 8003, 8004 (local setup).
The failover test kills the replica listening on --kill-port to simulate a crash;
restart it afterwards.
"""

import sys
import json
import time
import random
import argparse
import statistics
import subprocess
import threading
import urllib.request
import urllib.error
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

# Windows consoles default to cp1252, which cannot print characters such as "≈" or "→"
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 127.0.0.1, not "localhost": on Windows localhost tries IPv6 first and the replicas
# listen on IPv4 only, which adds ~2 s to every direct request.
GATEWAY = "http://127.0.0.1:8080"
DIRECT = "http://127.0.0.1:8000"
RESULTS = {}


def call(url, method="GET", body=None, token=None, timeout=300):
    """Returns (status, upstream, seconds, json_or_None)."""
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, method=method, headers=headers,
                                 data=json.dumps(body).encode() if body is not None else None)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = r.read()
            return r.status, r.headers.get("X-Upstream", ""), time.perf_counter() - t0, _json(data)
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("X-Upstream", ""), time.perf_counter() - t0, _json(e.read())
    except Exception as e:
        return 0, f"error: {type(e).__name__}", time.perf_counter() - t0, None


def _json(b):
    try:
        return json.loads(b)
    except Exception:
        return None


def port_of(upstream: str) -> str:
    """Replica that answered. Host processes: '192.168.65.254:8003' → '8003'.
    Containers (all on :8000): '172.19.0.3:8000' → '172.19.0.3'.
    On failover nginx lists every server tried; the last one answered."""
    last = upstream.split(",")[-1].strip()
    if ":" not in last:
        return last or "gateway"
    host, port = last.rsplit(":", 1)
    return host if host.startswith("172.") else port


def register():
    user = f"gw_test_{random.randint(10000, 99999)}"
    s, _, _, d = call(f"{GATEWAY}/api/v1/auth/register", "POST",
                      {"username": user, "password": "Test12345!", "role": "user"})
    assert s == 200, f"register failed: {s} {d}"
    return user, d["access_token"]


# ── 1. Load distribution ─────────────────────────────────────────────────────
def test_distribution(n=60):
    # 4 parallel clients, paced to stay under the gateway's 20 req/s per-IP limit
    def one(_):
        time.sleep(0.25)
        return call(f"{GATEWAY}/health")
    with ThreadPoolExecutor(4) as ex:
        res = list(ex.map(one, range(n)))
    dist = Counter(port_of(u) for s, u, _, _ in res if s == 200)
    ok = sum(1 for r in res if r[0] == 200)
    print(f"[distribution] {ok}/{n} OK — requests per replica: {dict(sorted(dist.items()))}")
    RESULTS["distribution"] = {"requests": n, "ok": ok, "per_replica": dict(sorted(dist.items()))}


# ── 2. Gateway overhead ──────────────────────────────────────────────────────
def test_overhead(n=300):
    # Persistent (keep-alive) connections on both sides, so the comparison measures the
    # gateway hop itself and not TCP connection setup. Requests are paced at ~15/s:
    # faster than the gateway's 20 req/s per-IP limit, nginx would answer 429 itself
    # (without contacting a replica) and the "latency" would be meaningless.
    # Only 200 responses are timed; any other status aborts the test.
    import http.client

    def lat(port):
        c = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        for _ in range(5):
            c.request("GET", "/health"); c.getresponse().read(); time.sleep(0.07)
        xs = []
        for _ in range(n):
            t = time.perf_counter()
            c.request("GET", "/health")
            r = c.getresponse(); r.read()
            dt = (time.perf_counter() - t) * 1000
            assert r.status == 200, f"got {r.status} on :{port} — overhead test must not hit the rate limit"
            xs.append(dt)
            time.sleep(0.07)
        xs.sort()
        return statistics.median(xs), xs[int(n * 0.95)]
    d50, d95 = lat(8000)
    time.sleep(3)
    g50, g95 = lat(8080)
    print(f"[overhead] direct p50={d50:.1f} ms p95={d95:.1f} ms | via gateway p50={g50:.1f} ms "
          f"p95={g95:.1f} ms | added ≈ {g50 - d50:.1f} ms (p50)")
    RESULTS["overhead_ms"] = {"direct_p50": round(d50, 1), "direct_p95": round(d95, 1),
                              "gateway_p50": round(g50, 1), "gateway_p95": round(g95, 1),
                              "added_p50": round(g50 - d50, 1)}


# ── 3. Real chat requests in parallel ────────────────────────────────────────
def test_chat(n=6):
    user, token = register()
    questions = ["What is the max flight time of the DJI Air 3?",
                 "How do I calibrate the compass on the DJI Mini 4 Pro?",
                 "What does error code E001 mean?",
                 "What is the weight of the DJI Neo?",
                 "How do I update the firmware of the DJI Air 3?",
                 "What is the max wind resistance of the DJI Mini 4 Pro?"][:n]
    t0 = time.perf_counter()
    with ThreadPoolExecutor(n) as ex:
        res = list(ex.map(lambda q: call(f"{GATEWAY}/api/v1/chat", "POST",
                                         {"query": q, "user_id": user}, token), questions))
    wall = time.perf_counter() - t0
    dist = Counter(port_of(u) for s, u, _, _ in res if s == 200)
    lats = [round(r[2], 1) for r in res]
    ok = sum(1 for r in res if r[0] == 200)
    print(f"[chat] {ok}/{n} OK in {wall:.1f}s wall (latencies {lats}) — per replica: {dict(sorted(dist.items()))}")
    RESULTS["chat"] = {"requests": n, "ok": ok, "wall_seconds": round(wall, 1),
                       "latencies_s": lats, "per_replica": dict(sorted(dist.items()))}
    return user, token


# ── 4. Failover: kill a replica during continuous traffic ────────────────────
def kill_port(port: int):
    ps = (f"$p = (Get-NetTCPConnection -LocalPort {port} -State Listen -ErrorAction SilentlyContinue)"
          f".OwningProcess | Select-Object -First 1; if ($p) {{ "
          f"$parent = (Get-CimInstance Win32_Process -Filter \"ProcessId=$p\").ParentProcessId; "
          f"Stop-Process -Id $p -Force; Stop-Process -Id $parent -Force -ErrorAction SilentlyContinue; \"killed $p\" }}")
    out = subprocess.run(["powershell", "-NoProfile", "-Command", ps], capture_output=True, text=True)
    return out.stdout.strip()


def kill_container(name: str):
    """Crash a replica container (SIGKILL, no graceful shutdown)."""
    ip = subprocess.run(["docker", "inspect", "-f",
                         "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", name],
                        capture_output=True, text=True).stdout.strip()
    out = subprocess.run(["docker", "kill", name], capture_output=True, text=True)
    return f"killed container {name} ({ip})" if out.returncode == 0 else ""


def test_failover(kill_port_no=8003, duration=24, kill_at=8, workers=4, container=None):
    # 4 clients × ~4 req/s ≈ 16 req/s: below the gateway's 20 req/s per-IP limit,
    # so every non-200 is a real failover failure, not a rate-limit rejection.
    user, token = register()
    stop = time.perf_counter() + duration
    t_start = time.perf_counter()
    log, lock = [], threading.Lock()

    def loop():
        while time.perf_counter() < stop:
            # mix of a public and an authenticated endpoint (MongoDB read)
            url = random.choice([f"{GATEWAY}/health", f"{GATEWAY}/api/v1/conversations?user_id={user}"])
            s, u, dt, _ = call(url, token=token, timeout=30)
            with lock:
                log.append((time.perf_counter() - t_start, s, u, dt))
            time.sleep(0.25)

    threads = [threading.Thread(target=loop) for _ in range(workers)]
    [t.start() for t in threads]
    time.sleep(kill_at)
    msg = kill_container(container) if container else kill_port(kill_port_no)
    t_kill = time.perf_counter() - t_start
    target = container or f"replica on :{kill_port_no}"
    print(f"[failover] t={t_kill:.1f}s: {target}: {msg or 'NOT FOUND'}")
    [t.join() for t in threads]

    before = [r for r in log if r[0] < t_kill]
    after = [r for r in log if r[0] >= t_kill]
    failed = [r for r in log if r[1] != 200]
    retried = [r for r in after if "," in r[2]]
    def dist(rows):
        return dict(sorted(Counter(port_of(r[2]) for r in rows if r[1] == 200).items()))
    print(f"[failover] {len(log)} requests, {len(failed)} failed "
          f"({sorted(Counter(r[1] for r in failed).items()) if failed else 'none'})")
    print(f"[failover] before kill: {dist(before)}  |  after kill: {dist(after)}")
    print(f"[failover] {len(retried)} request(s) transparently retried on another replica by the gateway")
    RESULTS["failover"] = {"killed": container or kill_port_no, "kill_at_s": round(t_kill, 1),
                           "requests": len(log), "failed": len(failed),
                           "retried_by_gateway": len(retried),
                           "before_kill": dist(before), "after_kill": dist(after)}


# ── 5. Edge rate limiting ────────────────────────────────────────────────────
def test_ratelimit(n=120):
    with ThreadPoolExecutor(40) as ex:
        res = list(ex.map(lambda _: call(f"{GATEWAY}/health", timeout=10), range(n)))
    codes = Counter(r[0] for r in res)
    from_gateway = sum(1 for s, u, _, _ in res if s == 429 and not u)
    print(f"[ratelimit] {n} requests in a burst → {dict(codes)}; "
          f"{from_gateway} rejected at the gateway (never reached a replica)")
    RESULTS["ratelimit"] = {"burst": n, "status_counts": {str(k): v for k, v in codes.items()},
                            "rejected_at_gateway": from_gateway}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("tests", nargs="+",
                    choices=["all", "distribution", "overhead", "chat", "failover", "ratelimit"])
    ap.add_argument("--kill-port", type=int, default=8003)
    ap.add_argument("--kill-container", help="docker-kill this replica container instead (e.g. agent-a-2)")
    ap.add_argument("--json", help="write results to this file")
    a = ap.parse_args()
    order = ["distribution", "overhead", "chat", "ratelimit", "failover"]
    todo = order if "all" in a.tests else [t for t in order if t in a.tests]
    for t in todo:
        if t == "failover":
            test_failover(a.kill_port, container=a.kill_container)
        else:
            globals()[f"test_{t}"]()
        time.sleep(3)  # let the per-IP rate-limit window drain between tests
    if a.json:
        with open(a.json, "w") as f:
            json.dump(RESULTS, f, indent=2)
        print(f"results → {a.json}")

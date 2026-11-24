"""
Evaluation — what happens when the API gateway ITSELF fails (Docker Compose stack).

    python eval_gateway.py crash     # nginx process dies; Docker's restart policy restarts it
    python eval_gateway.py down      # gateway container down for 15 s (host/container loss)

Continuous traffic (GET /health through the gateway, ~10 req/s) runs for the whole test;
every request is logged with its time and result, so the outage window is measured from
the first failed request to the first successful one after it. Each event is also marked
on the Grafana dashboard (annotation), and results are appended to
experiments/results/eval_gateway.jsonl.
"""

import sys
import json
import time
import base64
import argparse
import subprocess
import threading
import urllib.request
import urllib.error
from pathlib import Path

GATEWAY = "http://127.0.0.1:8080"
CONTAINER = "gateway"
GRAFANA = "http://127.0.0.1:3000"
GRAFANA_AUTH = "admin:change-me"
RESULTS = Path(__file__).parent / "results" / "eval_gateway.jsonl"
T0 = time.perf_counter()


def now():
    return time.perf_counter() - T0


def log(msg):
    print(f"[t={now():5.1f}s] {msg}", flush=True)


def annotate(text):
    body = json.dumps({"dashboardUID": "dji-gateway-agent-a", "time": int(time.time() * 1000),
                       "tags": ["evaluation", "gateway"], "text": text}).encode()
    req = urllib.request.Request(f"{GRAFANA}/api/annotations", data=body, method="POST",
                                 headers={"Content-Type": "application/json", "Authorization":
                                          "Basic " + base64.b64encode(GRAFANA_AUTH.encode()).decode()})
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        log(f"(Grafana annotation not written: {e})")


def docker(*args):
    out = subprocess.run(["docker", *args], capture_output=True, text=True)
    return (out.stdout + out.stderr).strip()


def restart_count():
    return docker("inspect", "-f", "{{.RestartCount}}", CONTAINER)


def traffic(duration, results, stop):
    while now() < duration and not stop.is_set():
        t = now()
        try:
            with urllib.request.urlopen(f"{GATEWAY}/health", timeout=3) as r:
                results.append((t, r.status, ""))
        except urllib.error.HTTPError as e:
            results.append((t, e.code, ""))
        except Exception as e:
            results.append((t, 0, type(e).__name__))
        time.sleep(0.1)


def summarise(test, results, t_fail):
    ok = [r for r in results if r[1] == 200]
    failed = [r for r in results if r[1] != 200]
    first_fail = min((r[0] for r in failed), default=None)
    recovered = min((r[0] for r in ok if first_fail is not None and r[0] > first_fail), default=None)
    outage = round(recovered - first_fail, 1) if failed and recovered else None
    errors = sorted({r[2] or str(r[1]) for r in failed})
    log(f"RESULT: {len(results)} requests, {len(ok)} OK, {len(failed)} FAILED ({', '.join(errors) or 'none'})")
    if failed:
        log(f"RESULT: outage window {first_fail:.1f}s → {recovered:.1f}s = {outage} s without service")
    res = {"test": test, "requests": len(results), "ok": len(ok), "failed": len(failed),
           "errors": errors, "failure_injected_at_s": round(t_fail, 1),
           "first_failure_s": round(first_fail, 1) if first_fail else None,
           "recovered_at_s": round(recovered, 1) if recovered else None,
           "outage_s": outage, "ts": time.strftime("%Y-%m-%d %H:%M:%S")}
    RESULTS.parent.mkdir(exist_ok=True)
    with open(RESULTS, "a", encoding="utf-8") as f:
        f.write(json.dumps(res) + "\n")
    return res


def run(test, duration, inject):
    results, stop = [], threading.Event()
    log(f"gateway container '{CONTAINER}': restart count {restart_count()}; traffic starts (GET /health, ~10 req/s)")
    th = threading.Thread(target=traffic, args=(duration, results, stop), daemon=True)
    th.start()
    time.sleep(8)
    t_fail = now()
    inject()
    th.join()
    log(f"gateway container now: {docker('ps', '-a', '--filter', f'name=^{CONTAINER}$', '--format', '{{.Status}}')}, "
        f"restart count {restart_count()}")
    return summarise(test, results, t_fail)


def inject_crash():
    log("FAILURE: stopping the nginx master process inside the gateway (simulated crash)")
    annotate("Gateway process crashed")
    docker("exec", CONTAINER, "nginx", "-s", "stop")
    for _ in range(100):  # report when Docker has brought it back
        if docker("inspect", "-f", "{{.State.Running}}", CONTAINER) == "true" and restart_count() != "0":
            break
        time.sleep(0.1)
    log("Docker restart policy (unless-stopped) restarted the container")


def inject_down():
    log("FAILURE: gateway container stopped (no restart: simulates losing the gateway host)")
    annotate("Gateway down")
    docker("stop", "-t", "0", CONTAINER)
    time.sleep(15)
    log("operator restarts the gateway (docker start)")
    docker("start", CONTAINER)
    annotate("Gateway restarted")


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    ap = argparse.ArgumentParser()
    ap.add_argument("test", choices=["crash", "down"])
    a = ap.parse_args()
    if a.test == "crash":
        run("gateway_crash_auto_restart", 25, inject_crash)
    else:
        run("gateway_down_15s", 35, inject_down)

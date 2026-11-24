"""
Failure scenario: Agent B (pricing service) goes down, then comes back.

    python scenario_agent_b_outage.py              # full scenario (~3 min)
    python scenario_agent_b_outage.py --no-restart # leave Agent B stopped at the end

Both specialists that depend on Agent B are exercised: the pricing agent and the
tutorial agent (YouTube search). They share ONE circuit breaker, because the breaker
protects the dependency (Agent B), not the caller. Tutorial questions are sent with
use_bert=false: with the default use_bert=true, the BERT classifier (which only knows
rag / diagnostic / pricing) bypasses the LLM supervisor and tutorial questions never
reach the tutorial agent.

What it does (and what to watch in Grafana, dashboard "Gateway & Agent A"):
  1. baseline   — one pricing question with Agent B up                → answered
  2. outage     — Agent B is killed; 9 pricing + 3 tutorial questions → still HTTP 200
                  (graceful degradation), chat outcome "error", circuit
                  breakers open after 3 failures per replica, answers get faster
                  (no more waiting on the dead service)
  3. recovery   — Agent B is restarted; after the 30 s recovery timeout the
                  breakers let a probe through and close again            → answered

Watch: "Circuit breaker — Agent B" (Closed → Open → Closed), "Chat outcomes per
minute" (error line), "Responses by status" (stays green: users never see a 5xx),
"Chat questions by route" (pricing_agent, tutorial_agent). The script also writes "Agent B stopped" /
"Agent B restarted" annotations on the dashboard's time axis.

Local setup only: Agent B on 127.0.0.1:8011, gateway on :8080, replica containers
agent-a-1/2/3 (published on 8000/8003/8004), Grafana on :3000 (admin/admin).
"""

import os
import sys
import json
import time
import base64
import random
import argparse
import subprocess
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

GATEWAY = "http://127.0.0.1:8080"
REPLICAS = {"agent-a-1": 8000, "agent-a-2": 8003, "agent-a-3": 8004}
AGENT_B_PORT = 8011
GRAFANA = "http://127.0.0.1:3000"
DASHBOARD_UID = "dji-gateway-agent-a"

HERE = os.path.dirname(os.path.abspath(__file__))
AGENT_B_DIR = os.path.normpath(os.path.join(HERE, "..", "agent-system-b"))
PYTHON = os.getenv("AGENT_PYTHON", r"C:\Users\chahi\Desktop\AIML\aiml-env\Scripts\python.exe")

PRICING_QUESTIONS = [
    "How much does the DJI Mini 4 Pro cost?", "Where can I buy the DJI Air 3?",
    "What is the price of the DJI Neo?", "Is the DJI Mini 4 Pro Fly More Combo in stock?",
    "Where is the DJI Air 3 cheapest right now?", "How much is the DJI Mavic 3 Classic?",
    "What does DJI Care Refresh cost for the Mini 4 Pro?", "Best price for the DJI Neo Fly More Combo?",
    "Which store sells the DJI Air 3 Fly More Combo?", "How much is a spare battery for the DJI Air 3?",
    "Price of the DJI Mini 4 Pro with RC 2 controller?", "Where can I buy the DJI Neo in the US?",
]
TUTORIAL_QUESTIONS = [
    "Show me a video tutorial on calibrating the compass of the DJI Mini 4 Pro",
    "Find a YouTube tutorial for the first flight with the DJI Neo",
    "Video guide: how to update the firmware of the DJI Air 3",
    "Tutorial video on using QuickShots with the DJI Mini 4 Pro",
    "YouTube tutorial for setting up the DJI RC 2 controller",
]
T0 = time.time()


def log(msg):
    print(f"[t+{time.time() - T0:6.1f}s] {msg}", flush=True)


def http(method, url, body=None, token=None, timeout=300, basic=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if basic:
        headers["Authorization"] = "Basic " + base64.b64encode(basic.encode()).decode()
    req = urllib.request.Request(url, method=method, headers=headers,
                                 data=json.dumps(body).encode() if body is not None else None)
    t = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}"), time.perf_counter() - t, r.headers.get("X-Instance", "")
    except urllib.error.HTTPError as e:
        return e.code, None, time.perf_counter() - t, e.headers.get("X-Instance", "")
    except Exception as e:
        return 0, {"error": str(e)}, time.perf_counter() - t, ""


def annotate(text, tag):
    """Vertical marker on the Grafana dashboard (best effort)."""
    s, _, _, _ = http("POST", f"{GRAFANA}/api/annotations",
                      {"dashboardUID": DASHBOARD_UID, "time": int(time.time() * 1000),
                       "tags": ["scenario", tag], "text": text}, basic="admin:admin", timeout=10)
    if s != 200:
        log(f"(Grafana annotation not written: HTTP {s})")


def breaker_states():
    out = {}
    for name, p in REPLICAS.items():
        s, d, _, _ = http("GET", f"http://127.0.0.1:{p}/health", timeout=5)
        out[name] = (d or {}).get("circuit_breakers", {}).get("agent_system_b", {}).get("state", "down") if s == 200 else "down"
    return out


def ask(token, user, q):
    """q = (question, use_bert). Tutorial questions use use_bert=False (see module doc)."""
    q, use_bert = q
    s, d, dt, up = http("POST", f"{GATEWAY}/api/v1/chat",
                        {"query": q, "user_id": user, "use_bert": use_bert}, token)
    route = ((d or {}).get("metadata") or {}).get("route", "?")
    answer = ((d or {}).get("response") or "").replace("\n", " ")
    return s, dt, up or "gateway", route, answer   # up = X-Instance (replica name)


def wave(token, user, questions, label, parallel=3):
    with ThreadPoolExecutor(parallel) as ex:
        results = list(ex.map(lambda q: ask(token, user, q), questions))
    for (s, dt, rep, route, ans), _ in zip(results, questions):
        log(f"  {label}: HTTP {s} in {dt:5.1f}s via {rep} route={route} | {ans[:90]}")
    return results


def stop_agent_b():
    ps = (f"$p = (Get-NetTCPConnection -LocalPort {AGENT_B_PORT} -State Listen -ErrorAction SilentlyContinue)"
          ".OwningProcess | Select-Object -First 1; if ($p) { "
          "$parent = (Get-CimInstance Win32_Process -Filter \"ProcessId=$p\").ParentProcessId; "
          "Stop-Process -Id $p -Force; Stop-Process -Id $parent -Force -ErrorAction SilentlyContinue; \"killed $p\" } "
          "else { 'not running' }")
    return subprocess.run(["powershell", "-NoProfile", "-Command", ps], capture_output=True, text=True).stdout.strip()


def start_agent_b():
    logf = open(os.path.join(HERE, "agent-b-scenario.log"), "ab")
    flags = getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    subprocess.Popen([PYTHON, "-m", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", str(AGENT_B_PORT)],
                     cwd=AGENT_B_DIR, stdout=logf, stderr=subprocess.STDOUT, creationflags=flags,
                     env={**os.environ, "PYTHONIOENCODING": "utf-8"})
    for _ in range(60):
        s, _, _, _ = http("GET", f"http://127.0.0.1:{AGENT_B_PORT}/health", timeout=2)
        if s == 200:
            return True
        time.sleep(1)
    return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-restart", action="store_true")
    a = ap.parse_args()
    pq = [(q, True) for q in random.sample(PRICING_QUESTIONS, len(PRICING_QUESTIONS))]
    tq = [(q, False) for q in random.sample(TUTORIAL_QUESTIONS, len(TUTORIAL_QUESTIONS))]

    user = f"scenario_{random.randint(10000, 99999)}"
    s, d, _, _ = http("POST", f"{GATEWAY}/api/v1/auth/register", {"username": user, "password": "Test12345!"})
    assert s == 200, f"register failed: {s}"
    token = d["access_token"]
    log(f"user {user} registered; breakers: {breaker_states()}")

    log("PHASE 1 — baseline with Agent B up")
    wave(token, user, [pq[0], tq[0]], "baseline", parallel=2)

    log(f"PHASE 2 — stopping Agent B: {stop_agent_b()}")
    annotate("Agent B stopped", "outage")
    # 12 questions: each replica has its OWN breaker (threshold 3), and the gateway
    # spreads the load, so ~3 failures per replica (≈ 9+ in total) are needed before
    # every replica stops calling Agent B.
    wave(token, user, [pq[1], tq[1], pq[2], pq[3], tq[2], pq[4], pq[5], tq[3], pq[6], pq[9], pq[10], pq[11]],
         "outage", parallel=3)
    log(f"breakers after outage traffic: {breaker_states()}")

    if a.no_restart:
        log("Agent B left stopped (--no-restart). Restart it to let the breakers close.")
        return

    log("PHASE 3 — restarting Agent B")
    ok = start_agent_b()
    annotate("Agent B restarted", "recovery")
    log(f"Agent B {'is back' if ok else 'did NOT come back'}; waiting 35 s (breaker recovery timeout 30 s)…")
    time.sleep(35)
    wave(token, user, [pq[7], tq[4], pq[8]], "recovery", parallel=3)
    log(f"breakers after recovery: {breaker_states()}")
    log("done — open Grafana → 'Gateway & Agent A', time range 'Last 15 minutes'")


if __name__ == "__main__":
    main()

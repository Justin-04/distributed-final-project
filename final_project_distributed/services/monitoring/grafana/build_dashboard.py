"""
Generates grafana/dashboards/gateway-agent-a.json (provisioned into Grafana).

    python build_dashboard.py

Colours (validated for the dark theme: lightness band, chroma, colour-blind separation,
contrast): replicas use the categorical order blue / orange / aqua, fixed per replica so
a replica keeps its colour when another one disappears; HTTP outcomes use the reserved
status colours (good / warning / critical), never the series colours.
"""

import json
import os

DS = {"type": "prometheus", "uid": "prometheus"}
REPLICA_COLORS = {"agent-a-1": "#3987e5", "agent-a-2": "#d95926", "agent-a-3": "#199e70"}
GOOD, WARNING, SERIOUS, CRITICAL = "#0ca30c", "#fab219", "#ec835a", "#d03b3b"
NEUTRAL = "#8a8a86"

_id = 0


def _next_id():
    global _id
    _id += 1
    return _id


def target(expr, legend="", ref="A", instant=False):
    t = {"datasource": DS, "expr": expr, "legendFormat": legend, "refId": ref, "range": not instant}
    if instant:
        t["instant"] = True
    return t


def replica_overrides():
    return [{"matcher": {"id": "byName", "options": r},
             "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": c}}]}
            for r, c in REPLICA_COLORS.items()]


def row(title, y):
    return {"id": _next_id(), "type": "row", "title": title, "collapsed": False,
            "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []}


def stat(title, expr, x, y, w=4, h=4, unit="short", decimals=None, thresholds=None,
         mappings=None, description="", color_mode="value"):
    steps = thresholds or [{"color": NEUTRAL, "value": None}]
    defaults = {"unit": unit, "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": steps}}
    if decimals is not None:
        defaults["decimals"] = decimals
    if mappings:
        defaults["mappings"] = mappings
    return {"id": _next_id(), "type": "stat", "title": title, "description": description,
            "datasource": DS, "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "targets": [target(expr, instant=True)],
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": color_mode, "graphMode": "none", "textMode": "auto",
                        "justifyMode": "center", "orientation": "auto"}}


def timeseries(title, targets, x, y, w=12, h=8, unit="short", overrides=None, description="",
               stack=False, fill=0, min0=True):
    defaults = {
        "unit": unit,
        "color": {"mode": "palette-classic"},
        "custom": {"lineWidth": 2, "fillOpacity": fill, "showPoints": "never",
                   "spanNulls": True, "axisSoftMin": 0 if min0 else None,
                   "stacking": {"mode": "normal" if stack else "none", "group": "A"},
                   "gradientMode": "none", "axisBorderShow": False},
    }
    return {"id": _next_id(), "type": "timeseries", "title": title, "description": description,
            "datasource": DS, "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": targets,
            "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
            "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                        "tooltip": {"mode": "multi", "sort": "desc"}}}


def bargauge(title, expr, legend, x, y, w=8, h=8, description=""):
    return {"id": _next_id(), "type": "bargauge", "title": title, "description": description,
            "datasource": DS, "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "targets": [target(expr, legend, instant=True)],
            "fieldConfig": {"defaults": {"unit": "short", "decimals": 0, "min": 0,
                                         "color": {"mode": "fixed", "fixedColor": "#3987e5"}},
                            "overrides": []},
            "options": {"orientation": "horizontal", "displayMode": "basic", "showUnfilled": True,
                        "valueMode": "text", "namePlacement": "left",
                        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}}}


GW = "gateway_http_response_count_total"
HIST = "gateway_http_response_time_seconds_hist_bucket"

panels = []
y = 0

# ── Row 1: API gateway ──────────────────────────────────────────────────────
panels.append(row("API gateway — every request entering the system", y)); y += 1
panels += [
    stat("Requests / s", f"sum(rate({GW}[1m]))", 0, y, unit="reqps", decimals=1,
         description="All requests through the gateway, last minute."),
    stat("Server errors (5xx)", f'(sum(rate({GW}{{status=~"5.."}}[5m])) or vector(0)) / clamp_min(sum(rate({GW}[5m])), 1e-9)',
         4, y, unit="percentunit", decimals=1,
         thresholds=[{"color": GOOD, "value": None}, {"color": WARNING, "value": 0.01},
                     {"color": CRITICAL, "value": 0.05}],
         description="Share of requests answered with 5xx in the last 5 minutes (e.g. 502 when a replica crashes mid-request)."),
    stat("Rate-limited (429), 15 min", f'sum(increase({GW}{{status="429"}}[15m])) or vector(0)', 8, y,
         decimals=0, thresholds=[{"color": NEUTRAL, "value": None}, {"color": WARNING, "value": 1}],
         description="Requests rejected by the gateway's per-IP limit; they never reach a replica."),
    stat("Failover retries, 15 min", f'sum(increase({GW}{{retried="yes"}}[15m])) or vector(0)', 12, y,
         decimals=0, thresholds=[{"color": NEUTRAL, "value": None}, {"color": SERIOUS, "value": 1}],
         description="Requests the gateway re-sent to another replica because the first one did not answer."),
    stat("Open connections", "sum(nginx_connections_active)", 16, y, decimals=0,
         description="Client connections currently open on the gateway (nginx stub_status)."),
    stat("Chat p95 at gateway", f'histogram_quantile(0.95, sum by (le) (rate({HIST}{{endpoint="chat"}}[5m])))',
         20, y, unit="s", decimals=1, description="95th percentile of chat response time seen by users, last 5 minutes."),
]
y += 4
panels += [
    timeseries("Requests per replica (gateway view)",
               [target(f'sum by (replica) (rate({GW}{{replica!="gateway"}}[1m]))', "{{replica}}")],
               0, y, unit="reqps", overrides=replica_overrides(),
               description="Where the gateway sends traffic. When a replica crashes its line drops to zero and the others rise."),
    timeseries("Responses by status",
               [target(f'sum by (status) (rate({GW}[1m]))', "{{status}}")],
               12, y, unit="reqps", stack=True, fill=30,
               overrides=[{"matcher": {"id": "byRegexp", "options": "^2.."},
                           "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": GOOD}}]},
                          {"matcher": {"id": "byRegexp", "options": "^4.."},
                           "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": WARNING}}]},
                          {"matcher": {"id": "byName", "options": "429"},
                           "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": SERIOUS}}]},
                          {"matcher": {"id": "byRegexp", "options": "^5.."},
                           "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": CRITICAL}}]}],
               description="2xx success · 4xx client errors (e.g. 401 bad login) · 429 rejected by the gateway's rate limit · 5xx server errors."),
]
y += 8
panels += [
    timeseries("Chat latency per replica — p95 (gateway view)",
               [target(f'histogram_quantile(0.95, sum by (le, replica) (rate({HIST}{{endpoint="chat",replica!="gateway"}}[5m])))',
                       "{{replica}}")],
               0, y, unit="s", overrides=replica_overrides(),
               description="Time from the gateway receiving a chat question to the answer, per replica."),
    timeseries("Retried requests (failover)",
               [target(f'sum(rate({GW}{{retried="yes"}}[1m])) * 60', "retried / min")],
               12, y, unit="short",
               overrides=[{"matcher": {"id": "byName", "options": "retried / min"},
                           "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": SERIOUS}}]}],
               description="Requests moved to another replica by the gateway. Non-zero only around a replica failure."),
]
y += 8

# ── Row 2: Agent A replicas ─────────────────────────────────────────────────
panels.append(row("Agent A replicas — measured inside each replica", y)); y += 1
panels += [
    stat("Replicas up", 'sum(up{job="agent-a"})', 0, y, decimals=0,
         thresholds=[{"color": CRITICAL, "value": None}, {"color": WARNING, "value": 2},
                     {"color": GOOD, "value": 3}],
         description="Replicas answering Prometheus scrapes."),
    stat("Chat questions, 1 h", "sum(increase(agent_a_chat_requests_total[1h])) or vector(0)", 4, y, decimals=0),
    stat("Response-cache hit ratio, 1 h",
         'sum(increase(agent_a_chat_requests_total{cache_hit="true"}[1h])) / clamp_min(sum(increase(agent_a_chat_requests_total[1h])), 1e-9)',
         8, y, unit="percentunit", decimals=0,
         description="Questions answered from the shared Redis cache without running the pipeline."),
    stat("Blocked by input guard, 1 h", 'sum(increase(agent_a_chat_requests_total{outcome="blocked"}[1h])) or vector(0)',
         12, y, decimals=0, thresholds=[{"color": NEUTRAL, "value": None}, {"color": WARNING, "value": 1}]),
    stat("Circuit breaker — MCP server", 'max(agent_a_circuit_breaker_state{service="mcp_server"})', 16, y,
         mappings=[{"type": "value", "options": {"0": {"text": "Closed", "color": GOOD},
                                                 "1": {"text": "Half-open", "color": WARNING},
                                                 "2": {"text": "Open", "color": CRITICAL}}}],
         color_mode="background", description="Worst state across replicas (each replica has its own breaker)."),
    stat("Circuit breaker — Agent B", 'max(agent_a_circuit_breaker_state{service="agent_system_b"})', 20, y,
         mappings=[{"type": "value", "options": {"0": {"text": "Closed", "color": GOOD},
                                                 "1": {"text": "Half-open", "color": WARNING},
                                                 "2": {"text": "Open", "color": CRITICAL}}}],
         color_mode="background"),
]
y += 4
BREAKER_MAPPINGS = [{"type": "value", "options": {
    "0": {"text": "Closed", "color": GOOD, "index": 0},
    "1": {"text": "Half-open", "color": WARNING, "index": 1},
    "2": {"text": "Open", "color": CRITICAL, "index": 2}}}]
for i, (svc, label) in enumerate((("agent_system_b", "Agent B"), ("mcp_server", "MCP server"))):
    panels.append({
        "id": _next_id(), "type": "state-timeline", "datasource": DS,
        "title": f"Circuit breaker over time — {label} (per replica)",
        "description": "Each replica has its own breaker: Closed → Open after 3 consecutive failures, "
                       "Half-open after 30 s (one probe call), Closed again when the probe succeeds.",
        "gridPos": {"x": 12 * i, "y": y, "w": 12, "h": 6},
        "targets": [target(f'max by (replica) (agent_a_circuit_breaker_state{{service="{svc}"}})', "{{replica}}")],
        "fieldConfig": {"defaults": {"mappings": BREAKER_MAPPINGS, "color": {"mode": "thresholds"},
                                     "thresholds": {"mode": "absolute", "steps": [
                                         {"color": GOOD, "value": None}, {"color": WARNING, "value": 1},
                                         {"color": CRITICAL, "value": 2}]},
                                     "custom": {"fillOpacity": 80, "lineWidth": 0}},
                        "overrides": []},
        "options": {"showValue": "never", "mergeValues": True, "alignValue": "left", "rowHeight": 0.8,
                    "legend": {"showLegend": False}, "tooltip": {"mode": "single"}},
    })
y += 6
panels += [
    timeseries("Requests in progress per replica",
               [target('sum by (replica) (agent_a_http_requests_in_progress)', "{{replica}}")],
               0, y, unit="short", overrides=replica_overrides(),
               description="Concurrent requests inside each replica: shows that least-connections balancing keeps them even."),
    timeseries("Requests per replica (replica view)",
               [target('sum by (replica) (rate(agent_a_http_requests_total[1m]))', "{{replica}}")],
               12, y, unit="reqps", overrides=replica_overrides(),
               description="Same traffic as the gateway panel, measured by the replicas themselves; the two views should agree."),
]
y += 8
panels += [
    timeseries("Chat latency (pipeline, no cache) — p50 / p95",
               [target('histogram_quantile(0.5, sum by (le) (rate(agent_a_chat_duration_seconds_bucket{cache_hit="false"}[5m])))', "p50", "A"),
                target('histogram_quantile(0.95, sum by (le) (rate(agent_a_chat_duration_seconds_bucket{cache_hit="false"}[5m])))', "p95", "B")],
               0, y, w=8, unit="s",
               overrides=[{"matcher": {"id": "byName", "options": "p50"},
                           "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "#3987e5"}}]},
                          {"matcher": {"id": "byName", "options": "p95"},
                           "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": "#d95926"}}]}]),
    bargauge("Chat questions by route, 1 h",
             'sum by (route) (increase(agent_a_chat_requests_total[1h]))', "{{route}}", 8, y,
             description="Which specialist agent the supervisor chose."),
    timeseries("Chat outcomes per minute",
               [target('sum by (outcome) (rate(agent_a_chat_requests_total[5m])) * 60', "{{outcome}}")],
               16, y, w=8, unit="short",
               description="Not stacked, so each outcome is read from the zero line.",
               overrides=[{"matcher": {"id": "byName", "options": "answered"},
                           "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": GOOD}}]},
                          {"matcher": {"id": "byName", "options": "blocked"},
                           "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": WARNING}}]},
                          {"matcher": {"id": "byName", "options": "error"},
                           "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": CRITICAL}}]}]),
]

dashboard = {
    "uid": "dji-gateway-agent-a",
    "title": "Gateway & Agent A",
    "description": "API gateway traffic and the Agent A replicas behind it.",
    "tags": ["dji", "gateway", "agent-a"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "editable": True,
    "graphTooltip": 1,
    "refresh": "5s",
    "time": {"from": "now-15m", "to": "now"},
    "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m", "5m"]},
    "panels": panels,
    # built-in annotation layer: shows markers written through the API
    # (e.g. "Agent B stopped" / "Agent B restarted" by scenario_agent_b_outage.py)
    "annotations": {"list": [{
        "builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"},
        "enable": True, "hide": True, "iconColor": "rgba(0, 211, 255, 1)",
        "name": "Annotations & Alerts", "type": "dashboard",
        "target": {"limit": 100, "matchAny": False, "tags": [], "type": "dashboard"}}]},
}

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboards", "gateway-agent-a.json")
os.makedirs(os.path.dirname(out), exist_ok=True)
with open(out, "w", encoding="utf-8") as f:
    json.dump(dashboard, f, indent=2, ensure_ascii=False)
print(f"written {out} ({len(panels)} panels)")

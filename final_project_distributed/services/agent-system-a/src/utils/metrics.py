"""
Prometheus metrics for an Agent A replica, exposed on GET /metrics.

Every replica exposes its own metrics; Prometheus scrapes each replica separately
(the `instance` label tells them apart), so per-replica load and latency can be
compared next to the gateway's view of the same traffic.

Metric families (all prefixed `agent_a_`):
  http_requests_total{method,path,status}      every API request
  http_request_duration_seconds{method,path}   latency histogram
  http_requests_in_progress                    concurrent requests on this replica
  chat_requests_total{route,cache_hit,outcome} chat questions by route chosen
  chat_duration_seconds{cache_hit}             end-to-end chat latency
  rate_limited_total                           per-user chat limit rejections
  circuit_breaker_state{service}               0 closed · 1 half-open · 2 open
"""

import time

from prometheus_client import (
    CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest,
)

HTTP_REQUESTS = Counter(
    "agent_a_http_requests_total", "HTTP requests handled by this replica",
    ["method", "path", "status"])
HTTP_LATENCY = Histogram(
    "agent_a_http_request_duration_seconds", "HTTP request latency",
    ["method", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 120))
IN_PROGRESS = Gauge(
    "agent_a_http_requests_in_progress", "Requests currently being processed by this replica")

CHAT_REQUESTS = Counter(
    "agent_a_chat_requests_total", "Chat questions answered",
    ["route", "cache_hit", "outcome"])
CHAT_DURATION = Histogram(
    "agent_a_chat_duration_seconds", "End-to-end chat latency on this replica",
    ["cache_hit"],
    buckets=(0.25, 0.5, 1, 2.5, 5, 10, 15, 20, 30, 45, 60, 90, 120, 180))
RATE_LIMITED = Counter(
    "agent_a_rate_limited_total", "Chat requests rejected by the per-user rate limiter")
CIRCUIT_STATE = Gauge(
    "agent_a_circuit_breaker_state", "Circuit breaker state: 0 closed, 1 half-open, 2 open",
    ["service"])

_STATE_VALUE = {"closed": 0, "half_open": 1, "half-open": 1, "open": 2}

# Create the expected label combinations at 0 when the replica starts. A counter series
# that first appears with a value of 1 has no "before" sample, so Prometheus' rate() and
# increase() cannot see that first event; starting every series at 0 avoids the gap.
for _hit in ("true", "false"):
    CHAT_DURATION.labels(_hit)
    for _route in ("rag_agent", "diagnostic_agent", "pricing_agent", "tutorial_agent", "summarizer"):
        for _outcome in ("answered", "blocked", "error"):
            CHAT_REQUESTS.labels(_route, _hit, _outcome)


def record_chat(route: str, cache_hit: bool, outcome: str, started: float):
    """outcome: answered | blocked (input guard) | error"""
    hit = "true" if cache_hit else "false"
    CHAT_REQUESTS.labels(route or "none", hit, outcome).inc()
    CHAT_DURATION.labels(hit).observe(time.perf_counter() - started)


def _refresh_circuit_breakers():
    try:
        from middleware.breakers import mcp_circuit_breaker, agent_b_circuit_breaker
        for name, cb in (("mcp_server", mcp_circuit_breaker), ("agent_system_b", agent_b_circuit_breaker)):
            state = str(cb.status.get("state", "closed")).lower()
            CIRCUIT_STATE.labels(name).set(_STATE_VALUE.get(state, 0))
    except Exception:
        pass


def metrics_response():
    """Body + content type for GET /metrics (circuit breakers read at scrape time)."""
    _refresh_circuit_breakers()
    return generate_latest(), CONTENT_TYPE_LATEST


class PrometheusMiddleware:
    """
    Pure ASGI middleware (works with streamed SSE responses, unlike BaseHTTPMiddleware).
    The path label is the route template (e.g. /api/v1/conversations/{conversation_id}),
    never the raw URL, so IDs do not create unbounded label values.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("path") == "/metrics":
            await self.app(scope, receive, send)
            return

        status = {"code": 500}
        started = time.perf_counter()

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                status["code"] = message["status"]
            await send(message)

        IN_PROGRESS.inc()
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            IN_PROGRESS.dec()
            route = scope.get("route")
            path = getattr(route, "path", None) or "unmatched"
            method = scope.get("method", "GET")
            HTTP_REQUESTS.labels(method, path, str(status["code"])).inc()
            HTTP_LATENCY.labels(method, path).observe(time.perf_counter() - started)

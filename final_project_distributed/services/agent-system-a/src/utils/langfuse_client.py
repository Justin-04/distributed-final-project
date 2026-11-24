"""
Langfuse client utility for fetching traces and observations.
Used by admin endpoints to display telemetry data.

Targets Langfuse v4 running in `events_only` mode, where the legacy
/api/public/traces endpoint is disabled. All data is read from
GET /api/public/v2/observations (cursor-paginated), then grouped by traceId.
"""

import os
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
import httpx

logger = logging.getLogger(__name__)


class LangfuseClient:
    """Client for fetching Langfuse observations via the v4 public REST API."""

    def __init__(self):
        self.base_url = os.getenv("LANGFUSE_BASE_URL", "http://localhost:5001").rstrip("/")
        self.public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
        self.secret_key = os.getenv("LANGFUSE_SECRET_KEY")
        self.available = bool(self.public_key and self.secret_key)

        if not self.available:
            logger.warning("Langfuse credentials not configured")

    async def _fetch_observations_raw(
        self,
        from_start_time: datetime,
        to_start_time: datetime,
        limit: int = 100,
        max_records: int = 1000,
        trace_id: Optional[str] = None,
    ) -> List[Dict]:
        """
        Fetch raw observations from the v4 API, following cursor pagination.

        Returns a flat list of observation objects.
        """
        if not self.available:
            return []

        observations: List[Dict] = []
        cursor: Optional[str] = None

        # Request all field groups so io/usage/cost/metadata/model are returned.
        # Without this, the v4 API only returns "core,basic" (no tokens/cost/io).
        fields = "core,basic,io,usage,metadata,model,metrics"

        try:
            async with httpx.AsyncClient(timeout=30) as client:
                while len(observations) < max_records:
                    params = {
                        "fromStartTime": from_start_time.isoformat() + "Z"
                        if from_start_time.tzinfo is None
                        else from_start_time.isoformat(),
                        "toStartTime": to_start_time.isoformat() + "Z"
                        if to_start_time.tzinfo is None
                        else to_start_time.isoformat(),
                        "limit": min(limit, 100),
                        "fields": fields,
                    }
                    if cursor:
                        params["cursor"] = cursor
                    if trace_id:
                        params["traceId"] = trace_id

                    response = await client.get(
                        f"{self.base_url}/api/public/v2/observations",
                        params=params,
                        auth=(self.public_key, self.secret_key),
                    )
                    response.raise_for_status()
                    payload = response.json()

                    batch = payload.get("data", [])
                    if not batch:
                        break
                    observations.extend(batch)

                    cursor = payload.get("meta", {}).get("cursor")
                    if not cursor:
                        break

            return observations

        except Exception as e:
            logger.error(f"Failed to fetch Langfuse observations: {e}")
            return observations

    @staticmethod
    def _latency_ms(obs: Dict) -> float:
        """Observation latency is reported in seconds in v4; convert to ms."""
        latency = obs.get("latency")
        if latency is None:
            return 0.0
        return round(float(latency) * 1000, 2)

    def _group_into_traces(self, observations: List[Dict]) -> List[Dict]:
        """
        Group observations by traceId into synthetic trace records.

        Each trace aggregates: earliest start, latest end, total latency,
        total cost/tokens, and picks the root observation for name/user/session.
        """
        traces: Dict[str, Dict] = {}

        for obs in observations:
            trace_id = obs.get("traceId")
            if not trace_id:
                continue

            if trace_id not in traces:
                traces[trace_id] = {
                    "id": trace_id,
                    "timestamp": obs.get("startTime"),
                    "name": obs.get("name", "unknown"),
                    "user_id": obs.get("userId") or "",
                    "session_id": obs.get("sessionId") or "",
                    "input": obs.get("input"),
                    "output": obs.get("output"),
                    "duration_ms": 0.0,
                    "total_cost": 0.0,
                    "total_tokens": 0,
                    "metadata": obs.get("metadata", {}) or {},
                    "_observation_count": 0,
                }

            t = traces[trace_id]
            t["_observation_count"] += 1
            t["duration_ms"] += self._latency_ms(obs)

            # Cost + tokens (Langfuse v4 field names)
            t["total_cost"] += float(obs.get("totalCost") or 0.0)
            t["total_tokens"] += int(obs.get("totalUsage") or 0)

            # Prefer the root observation for identity fields
            if obs.get("isRootObservation"):
                t["timestamp"] = obs.get("startTime", t["timestamp"])
                t["name"] = obs.get("name", t["name"])
                t["user_id"] = obs.get("userId") or t["user_id"]
                t["session_id"] = obs.get("sessionId") or t["session_id"]
                if obs.get("input") is not None:
                    t["input"] = obs.get("input")
                if obs.get("output") is not None:
                    t["output"] = obs.get("output")
                if obs.get("metadata"):
                    t["metadata"] = obs.get("metadata")

        # Derive route/intent. The v4 list API does not return metadata, so
        # fall back to parsing the observation name (e.g. "agent-system-a-rag_agent").
        result = []
        for t in traces.values():
            meta = t.get("metadata") or {}
            name = t.get("name") or ""

            route = meta.get("route")
            if not route:
                if name.startswith("agent-system-a-"):
                    route = name[len("agent-system-a-"):] or "unknown"
                elif "OpenAI" in name or "generation" in name.lower():
                    route = "pricing_agent"  # Agent-B LLM generations
                else:
                    route = "unknown"

            t["route"] = route
            t["intent"] = meta.get("intent", route.replace("_agent", "") if route != "unknown" else "unknown")
            t["total_cost"] = round(t["total_cost"], 6)
            t["duration_ms"] = round(t["duration_ms"], 2)
            result.append(t)

        # Sort newest first
        result.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
        return result

    async def get_traces(
        self,
        limit: int = 50,
        page: int = 1,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        user_id: Optional[str] = None,
    ) -> Dict:
        """
        Fetch traces (grouped from observations) with client-side pagination.

        Returns:
            {"data": [trace...], "meta": {page, limit, totalItems, totalPages}}
        """
        if not self.available:
            return {"data": [], "meta": {"page": page, "limit": limit, "totalItems": 0, "totalPages": 0}}

        from_ts = from_timestamp or (datetime.utcnow() - timedelta(days=7))
        to_ts = to_timestamp or datetime.utcnow()

        observations = await self._fetch_observations_raw(from_ts, to_ts, max_records=1000)
        traces = self._group_into_traces(observations)

        if user_id:
            traces = [t for t in traces if t.get("user_id") == user_id]

        total_items = len(traces)
        total_pages = (total_items + limit - 1) // limit if limit else 1
        start = (page - 1) * limit
        end = start + limit
        page_slice = traces[start:end]

        return {
            "data": page_slice,
            "meta": {
                "page": page,
                "limit": limit,
                "totalItems": total_items,
                "totalPages": total_pages,
            },
        }

    async def get_trace_details(self, trace_id: str) -> Optional[Dict]:
        """
        Fetch a single trace by grouping its observations.

        Uses a wide time window since events_only mode has no per-trace endpoint.
        """
        if not self.available:
            return None

        from_ts = datetime.utcnow() - timedelta(days=90)
        to_ts = datetime.utcnow()

        observations = await self._fetch_observations_raw(
            from_ts, to_ts, max_records=1000, trace_id=trace_id
        )
        if not observations:
            return None

        traces = self._group_into_traces(observations)
        return traces[0] if traces else None

    async def get_observations(
        self,
        trace_id: Optional[str] = None,
        limit: int = 100,
        page: int = 1,
    ) -> Dict:
        """
        Fetch raw observations (spans, generations) for a trace.

        Returns:
            {"data": [observation...], "meta": {...}}
        """
        if not self.available:
            return {"data": [], "meta": {"page": page, "limit": limit, "totalItems": 0, "totalPages": 0}}

        from_ts = datetime.utcnow() - timedelta(days=90)
        to_ts = datetime.utcnow()

        observations = await self._fetch_observations_raw(
            from_ts, to_ts, max_records=1000, trace_id=trace_id
        )

        return {
            "data": observations,
            "meta": {
                "page": 1,
                "limit": len(observations),
                "totalItems": len(observations),
                "totalPages": 1,
            },
        }

    async def get_usage_stats(
        self,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
    ) -> Dict:
        """
        Calculate aggregated usage statistics from grouped traces.
        """
        empty = {
            "total_requests": 0,
            "total_tokens": 0,
            "total_cost": 0.0,
            "avg_latency_ms": 0.0,
            "requests_by_route": {},
            "tokens_by_model": {},
        }

        if not self.available:
            return empty

        from_ts = from_timestamp or (datetime.utcnow() - timedelta(days=7))
        to_ts = to_timestamp or datetime.utcnow()

        observations = await self._fetch_observations_raw(from_ts, to_ts, max_records=1000)
        if not observations:
            return empty

        traces = self._group_into_traces(observations)

        total_requests = len(traces)
        total_tokens = 0
        total_cost = 0.0
        total_latency_ms = 0.0
        requests_by_route: Dict[str, int] = {}

        # Per-model token breakdown from raw observations
        tokens_by_model: Dict[str, int] = {}
        for obs in observations:
            model = obs.get("model") or obs.get("modelId")
            if model:
                tokens_by_model[model] = tokens_by_model.get(model, 0) + int(
                    obs.get("totalUsage") or 0
                )

        for t in traces:
            total_tokens += t.get("total_tokens", 0)
            total_cost += t.get("total_cost", 0.0)
            total_latency_ms += t.get("duration_ms", 0.0)
            route = t.get("route", "unknown")
            requests_by_route[route] = requests_by_route.get(route, 0) + 1

        avg_latency_ms = total_latency_ms / total_requests if total_requests > 0 else 0.0

        return {
            "total_requests": total_requests,
            "total_tokens": total_tokens,
            "total_cost": round(total_cost, 6),
            "avg_latency_ms": round(avg_latency_ms, 2),
            "requests_by_route": requests_by_route,
            "tokens_by_model": tokens_by_model,
        }


# Global instance
langfuse_client = LangfuseClient()

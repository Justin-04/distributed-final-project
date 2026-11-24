"""
Langfuse tracing integration for the LangGraph workflow.

Targets the Langfuse Python SDK v4.x (works with Langfuse server v4).
The v4 SDK uses OpenTelemetry-style observations created via
`start_as_current_observation(...)` rather than the removed v2 `.trace()` method.
Trace-level attributes (user_id, session_id) are set with `update_current_trace()`.
"""

import os
import logging
import threading
from contextlib import contextmanager
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Try to import Langfuse v4 SDK
try:
    from langfuse import Langfuse, propagate_attributes
    _langfuse_available = True
except ImportError:
    logger.warning("Langfuse not installed. Install with: pip install 'langfuse>=4,<5'")
    _langfuse_available = False
    Langfuse = None
    propagate_attributes = None


# Phrases indicating a failed / empty answer (for success scoring)
_FAILURE_PHRASES = ["unavailable", "cannot provide", "no information", "no context available"]


class LangfuseTracer:
    """Sends one trace per graph execution to Langfuse (v4 SDK)."""

    def __init__(self):
        self.enabled = False
        self.client = None

        if not _langfuse_available:
            logger.info("Langfuse tracing disabled (package not installed)")
            return

        public_key = os.getenv("LANGFUSE_PUBLIC_KEY")
        secret_key = os.getenv("LANGFUSE_SECRET_KEY")
        base_url = os.getenv("LANGFUSE_BASE_URL", "http://localhost:5001")

        if not (public_key and secret_key):
            logger.info("Langfuse tracing disabled (credentials not configured)")
            return

        try:
            self.client = Langfuse(
                public_key=public_key,
                secret_key=secret_key,
                host=base_url,
            )
            self.enabled = True
            logger.info(f"Langfuse tracing enabled -> {base_url}")
        except Exception as e:
            logger.warning(f"Failed to initialize Langfuse: {e}")
            self.enabled = False

    def _safe_flush(self):
        """Upload pending traces; runs in a background thread, never raises."""
        try:
            if self.client is not None:
                self.client.flush()
        except Exception as e:
            logger.warning(f"Langfuse flush failed (tracing only, request unaffected): {e}")

    @contextmanager
    def trace_request(self, query: str, user_id: str, conversation_id: str, use_bert: bool = True):
        """
        Context manager that wraps a full graph execution in one trace.

        Any `langfuse.openai` LLM calls made inside this block (supervisor,
        RAG generation, summarizer, etc.) are captured as child generations
        with token usage + cost, so the trace shows real totals.

        Usage:
            with langfuse_tracer.trace_request(query, uid, cid, use_bert) as ctx:
                result = agent_graph.invoke(...)
                ctx["result"] = result   # let the tracer finalize trace attrs

        The yielded dict lets the caller hand back the graph result so trace
        name/route/scoring can be finalized on exit.
        """
        ctx: Dict[str, Any] = {"result": None, "trace_id": None}

        if not self.enabled:
            yield ctx
            return

        try:
            with propagate_attributes(
                user_id=user_id,
                session_id=conversation_id,
                trace_name="agent-system-a",
            ):
                with self.client.start_as_current_observation(
                    name="agent-system-a",
                    as_type="chain",
                    input={"query": query, "use_bert": use_bert},
                ) as span:
                    ctx["trace_id"] = self.client.get_current_trace_id()

                    # Hand control back to the caller to run the graph.
                    yield ctx

                    # Finalize using the graph result the caller stored.
                    result = ctx.get("result") or {}
                    route = result.get("route", "unknown")
                    intent = result.get("intent", "unknown")
                    final_response = result.get("final_response", "")
                    input_safe = result.get("input_safe", True)

                    metadata = {
                        "route": route,
                        "intent": intent,
                        "confidence": result.get("confidence", 0.0),
                        "iteration_count": result.get("iteration_count", 0),
                        "input_safe": input_safe,
                        "guardrail_message": result.get("guardrail_message"),
                        "user_id": user_id,
                        "conversation_id": conversation_id,
                    }

                    span.update(
                        name=f"agent-system-a-{route}",
                        output={"response": final_response},
                        metadata=metadata,
                    )
                    span.set_trace_io(
                        input={"query": query},
                        output={"response": final_response},
                    )

                    if not input_safe:
                        span.score_trace(name="blocked", value=1.0)
                    elif final_response and not any(
                        p in final_response.lower() for p in _FAILURE_PHRASES
                    ):
                        span.score_trace(name="success", value=1.0)
                    else:
                        span.score_trace(name="success", value=0.0)

            # Export the trace WITHOUT blocking the request. This context manager exits
            # on the replica's event loop: a synchronous flush() waited for the upload to
            # Langfuse and, when Langfuse was down, froze the whole replica for seconds
            # per chat (health checks and Prometheus scrapes timed out). Telemetry must
            # never sit on the critical path, so the flush runs in a background thread.
            threading.Thread(target=self._safe_flush, name="langfuse-flush", daemon=True).start()

        except Exception as e:
            logger.error(f"Failed during Langfuse trace_request: {e}")
            # Ensure the caller's block still runs even if tracing setup failed
            if ctx.get("result") is None:
                yield ctx

    def trace_execution(
        self,
        query: str,
        user_id: str,
        conversation_id: str,
        result: Dict[str, Any],
        duration_ms: float,
    ) -> Optional[str]:
        """
        Backwards-compatible one-shot trace (does not capture child LLM calls).
        Prefer `trace_request` as a context manager around graph.invoke().
        """
        if not self.enabled:
            return None
        try:
            with self.trace_request(query, user_id, conversation_id, result.get("use_bert", True)) as ctx:
                ctx["result"] = result
            return ctx.get("trace_id")
        except Exception as e:
            logger.error(f"Failed to send Langfuse trace: {e}")
            return None


# Global tracer instance
langfuse_tracer = LangfuseTracer()

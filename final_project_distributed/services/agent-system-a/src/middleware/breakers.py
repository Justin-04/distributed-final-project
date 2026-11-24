"""
The circuit breakers of this process — ONE instance per downstream service.

Every caller of a service must use the same breaker object, otherwise each caller
counts failures separately and the breaker opens late (or never). They are defined
here, in the `middleware` package, because that package is always imported under the
same name (`middleware.*`). Defining them inside the agent modules was a bug: those
modules are imported both as `src.agents.x` (by the graph) and as `agents.x` (by other
modules), which created a second, never-used copy of each breaker that /health and the
Prometheus metrics were reporting.

    from middleware.breakers import mcp_circuit_breaker, agent_b_circuit_breaker
"""

from middleware.circuit_breaker import CircuitBreaker

# MCP server: used by the RAG agent and the diagnostic agent
mcp_circuit_breaker = CircuitBreaker("mcp-server", failure_threshold=3, recovery_timeout=30)

# Agent System B: used by the pricing agent and the tutorial agent
agent_b_circuit_breaker = CircuitBreaker("agent-system-b", failure_threshold=3, recovery_timeout=30)

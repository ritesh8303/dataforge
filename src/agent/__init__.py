"""In-process multi-agent job matching (Supervisor + specialists)."""

from agent.graph import HAS_LANGGRAPH, run_match_agent

__all__ = ["run_match_agent", "HAS_LANGGRAPH"]

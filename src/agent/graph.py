"""Multi-agent match graph — LangGraph when available, sequential fallback otherwise."""

from __future__ import annotations

from typing import Any

from agent.hitl import enqueue_review
from agent.nodes import (
    critic_node,
    explainer_node,
    retriever_node,
    scorer_node,
    should_retry_explainer,
    supervisor_node,
)
from agent.state import MAX_HANDOFFS, MAX_LLM_CALLS, AgentState

try:
    from langgraph.graph import END, StateGraph  # type: ignore

    HAS_LANGGRAPH = True
except ImportError:  # pragma: no cover
    HAS_LANGGRAPH = False
    END = "end"
    StateGraph = None  # type: ignore


def _run_sequential(state: AgentState) -> AgentState:
    state = supervisor_node(state)
    state = retriever_node(state)
    state = scorer_node(state)
    state = explainer_node(state)
    state = critic_node(state)
    if should_retry_explainer(state) == "explain":
        state = explainer_node(state)
        state = critic_node(state)
    return state


def _compile_langgraph():
    graph = StateGraph(dict)
    graph.add_node("supervisor", supervisor_node)
    graph.add_node("retrieve", retriever_node)
    graph.add_node("score", scorer_node)
    graph.add_node("explain", explainer_node)
    graph.add_node("critic", critic_node)
    graph.set_entry_point("supervisor")
    graph.add_edge("supervisor", "retrieve")
    graph.add_edge("retrieve", "score")
    graph.add_edge("score", "explain")
    graph.add_edge("explain", "critic")
    graph.add_conditional_edges("critic", should_retry_explainer, {"explain": "explain", "end": END})
    return graph.compile()


_COMPILED = None


def get_graph():
    global _COMPILED
    if not HAS_LANGGRAPH:
        return None
    if _COMPILED is None:
        _COMPILED = _compile_langgraph()
    return _COMPILED


def run_match_agent(
    *,
    resume: str = "",
    dream_role: str = "",
    location: str = "",
    limit: int = 15,
    visa_status: str = "",
    german_level: str = "",
    entry_level_only: bool = False,
    english_ok_only: bool = False,
    tech_only: bool = True,
) -> dict[str, Any]:
    if not resume and not dream_role:
        raise ValueError("resume or dream_role required")

    initial: AgentState = {
        "resume": resume,
        "dream_role": dream_role,
        "location": location,
        "limit": limit,
        "visa_status": visa_status,
        "german_level": german_level,
        "entry_level_only": entry_level_only,
        "english_ok_only": english_ok_only,
        "tech_only": tech_only,
        "llm_calls": 0,
        "handoffs": 0,
        "explainer_retries": 0,
        "errors": [],
    }

    graph = get_graph()
    if graph is not None:
        final = graph.invoke(initial)
    else:
        final = _run_sequential(initial)

    jobs = list(final.get("explanations") or final.get("scored") or [])
    payload = {
        "jobs": jobs,
        "count": len(jobs),
        "method": "multi_agent_langgraph" if HAS_LANGGRAPH else "multi_agent_sequential",
        "plan": final.get("plan"),
        "critic_ok": bool(final.get("critic_ok")),
        "critic_notes": final.get("critic_notes") or "",
        "llm_calls": int(final.get("llm_calls") or 0),
        "handoffs": int(final.get("handoffs") or 0),
        "limits": {"max_llm_calls": MAX_LLM_CALLS, "max_handoffs": MAX_HANDOFFS},
        "pii_redacted": final.get("pii_redacted") or {},
        "filters_applied": {
            "visa_status": visa_status or None,
            "german_level": german_level or None,
            "entry_level_only": entry_level_only,
            "english_ok_only": english_ok_only,
            "tech_only": tech_only,
            "location": location or None,
        },
        "hitl": bool(final.get("hitl")),
        "errors": final.get("errors") or [],
        "disclaimer": (
            "Multi-agent match is advisory. Visa flags are not legal advice. "
            "Low-confidence results may be queued for human review."
        ),
        "cost_summary": final.get("cost_summary") or {},
        "backend": "langgraph" if HAS_LANGGRAPH else "sequential",
    }

    if payload["hitl"]:
        dest = enqueue_review(
            {
                **payload,
                "dream_role": dream_role,
                "location": location,
                "visa_status": visa_status,
                "hitl_reason": payload.get("critic_notes") or "low_confidence",
            }
        )
        payload["hitl_queue"] = dest

    return payload

"""Additional multi-agent failure / guardrail cases."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import fastapi  # noqa: F401
import pydantic  # noqa: F401

from agent.graph import run_match_agent
from agent.nodes import critic_node, explainer_node, should_retry_explainer
from agent.state import AgentState


def test_missing_resume_and_dream_raises():
    try:
        run_match_agent(resume="", dream_role="")
        assert False, "expected ValueError"
    except ValueError as exc:
        assert "required" in str(exc).lower()


def test_critic_retry_then_end_on_budget():
    state: AgentState = {
        "critic_ok": False,
        "explainer_retries": 0,
        "handoffs": 5,
        "explanations": [{"job_id": "x", "citations": []}],
    }
    assert should_retry_explainer(state) == "explain"
    state["explainer_retries"] = 1
    assert should_retry_explainer(state) == "end"


def test_explainer_fills_citations_without_llm():
    state: AgentState = {
        "scored": [
            {
                "job_id": "j1",
                "title": "Junior DE",
                "description": "Python Spark",
                "ai_confidence": 0.9,
            }
        ],
        "resume": "Python",
        "dream_role": "Data Engineer",
        "llm_calls": 4,  # budget exhausted → no LLM polish
        "handoffs": 0,
        "limit": 5,
    }
    with patch("agent.nodes._router") as router:
        out = explainer_node(state)
        router.assert_not_called()
    assert out["explanations"][0]["citations"]
    assert out["explanations"][0]["citations"][0]["job_id"] == "j1"


def test_scorer_bad_json_does_not_crash():
    from agent.nodes import scorer_node

    jobs = [
        {
            "job_id": "j1",
            "title": "Junior Data Engineer",
            "description": "Python",
            "match_score": 0.5,
        }
    ]
    state: AgentState = {
        "candidates": jobs,
        "resume": "python",
        "dream_role": "DE",
        "llm_calls": 0,
        "handoffs": 0,
        "limit": 5,
    }
    mock_router = MagicMock()
    mock_router.complete.side_effect = RuntimeError("over budget")
    mock_router.cost_logger.summary.return_value = {}
    with patch("agent.nodes._router", return_value=mock_router):
        out = scorer_node(state)
    assert out["scored"]
    assert any("scorer" in e for e in (out.get("errors") or []))


def test_hitl_flag_on_low_confidence():
    state: AgentState = {
        "explanations": [
            {
                "job_id": "j1",
                "ai_confidence": 0.2,
                "citations": [{"job_id": "j1", "reason": "ok", "evidence": "x"}],
            }
        ],
        "handoffs": 0,
    }
    out = critic_node(state)
    assert out["critic_ok"] is True
    assert out["hitl"] is True

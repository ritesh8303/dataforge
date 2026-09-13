"""Tests for multi-agent graph, critic guardrails, HITL queue, MCP tools."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

# Prefer site-packages FastAPI/Pydantic over vendored Lambda copies under src/.
import fastapi  # noqa: F401
import pydantic  # noqa: F401

from agent.hitl import enqueue_review
from agent.nodes import critic_node, should_retry_explainer
from agent.state import AgentState


SAMPLE_JOBS = [
    {
        "job_id": "j1",
        "title": "Junior Data Engineer Python",
        "company": "N26",
        "location": "Berlin",
        "description": "Python Spark Airflow. English working language. Visa sponsorship available.",
        "tags": "Python,AWS",
        "is_tech": True,
        "ai_entry_level": True,
        "ai_english_ok": True,
        "ai_visa_stance": "sponsorship_offered",
        "ai_evidence_visa": "Visa sponsorship available",
        "match_score": 0.9,
    },
    {
        "job_id": "j2",
        "title": "Frontend React",
        "company": "Web",
        "location": "Hamburg",
        "description": "React TypeScript",
        "tags": "React",
        "is_tech": True,
        "ai_entry_level": True,
        "ai_english_ok": True,
        "ai_visa_stance": "not_mentioned",
        "match_score": 0.2,
    },
]


def test_critic_rejects_missing_citations():
    state: AgentState = {
        "explanations": [{"job_id": "j1", "citations": [], "ai_confidence": 0.9}],
        "handoffs": 0,
    }
    out = critic_node(state)
    assert out["critic_ok"] is False
    assert out["hitl"] is True


def test_critic_accepts_valid_citations():
    state: AgentState = {
        "explanations": [
            {
                "job_id": "j1",
                "ai_confidence": 0.85,
                "citations": [
                    {"job_id": "j1", "reason": "Python overlap", "evidence": "Python Spark"}
                ],
            }
        ],
        "handoffs": 0,
    }
    out = critic_node(state)
    assert out["critic_ok"] is True
    assert out["hitl"] is False


def test_retry_budget():
    state: AgentState = {"critic_ok": False, "explainer_retries": 0, "handoffs": 2}
    assert should_retry_explainer(state) == "explain"
    state["explainer_retries"] = 1
    assert should_retry_explainer(state) == "end"


def test_hitl_enqueue_local(tmp_path, monkeypatch):
    path = tmp_path / "hitl.csv"
    monkeypatch.setenv("HITL_REVIEW_LOCAL_PATH", str(path))
    monkeypatch.delenv("HITL_REVIEW_S3_PREFIX", raising=False)
    dest = enqueue_review({"dream_role": "Data Engineer", "jobs": [{"job_id": "j1"}], "hitl_reason": "low"})
    assert dest == str(path)
    assert path.exists()
    assert "Data Engineer" in path.read_text(encoding="utf-8")


def test_run_match_agent_with_mocks(tmp_path, monkeypatch):
    monkeypatch.setenv("HITL_REVIEW_LOCAL_PATH", str(tmp_path / "q.csv"))
    monkeypatch.setenv("AI_ENABLED", "true")

    index = [{"job_id": "j1", "vector": [1.0, 0.0], "model": "local", "provider": "local"}]

    with patch("agent.nodes.load_jobs_and_index", return_value=(SAMPLE_JOBS, index)):
        from agent.graph import run_match_agent

        result = run_match_agent(
            resume="Python AWS data engineer junior",
            dream_role="Data Engineer",
            location="Berlin",
            limit=5,
            visa_status="chancenkarte_or_job_seeker",
            entry_level_only=True,
        )
    assert result["count"] >= 1
    assert result["jobs"][0]["citations"]
    assert all(c["job_id"] for j in result["jobs"] for c in j["citations"])
    assert result["llm_calls"] <= 4
    assert result["handoffs"] <= 6
    assert "method" in result


def test_fastapi_agent_route(monkeypatch):
    from fastapi.testclient import TestClient
    from api.app import app

    monkeypatch.delenv("MATCH_API_KEY", raising=False)

    fake = {
        "jobs": [
            {
                **SAMPLE_JOBS[0],
                "citations": [{"job_id": "j1", "reason": "fit", "evidence": "Python"}],
            }
        ],
        "count": 1,
        "method": "multi_agent_sequential",
        "critic_ok": True,
        "hitl": False,
        "llm_calls": 1,
        "handoffs": 5,
        "disclaimer": "advisory",
        "cost_summary": {},
    }
    with patch("api.app.run_match_agent", return_value=fake):
        client = TestClient(app)
        res = client.post("/match/agent", json={"resume": "python", "dream_role": "Data Engineer"})
        assert res.status_code == 200
        assert res.json()["method"].startswith("multi_agent")


def test_mcp_tool_list():
    from mcp.server import TOOLS, _handle

    resp = _handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
    names = {t["name"] for t in resp["result"]["tools"]}
    assert names == {"search_jobs", "get_job", "match_resume"}
    assert "search_jobs" in TOOLS

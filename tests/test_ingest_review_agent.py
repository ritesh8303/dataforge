"""Tests for ingest-review agent (HITL automation)."""

from __future__ import annotations

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from agent.ingest_review_agent import review_row, run_ingest_review_agent, save_decisions
from processing.audience_gate import classify_for_audience


def test_rules_accept_werkstudent_data(tmp_path, monkeypatch):
    monkeypatch.setenv("INGEST_REVIEW_DECISIONS_PATH", str(tmp_path / "empty.csv"))
    row = {
        "job_id": "j-ws-data",
        "title": "Werkstudent:in Data Engineering",
        "company": "Acme",
        "location": "Berlin",
        "source": "ba_api",
        "field": "other_tech",
        "seniority": "working_student",
        "payload_json": "{}",
    }
    out = review_row(row, use_llm=False)
    assert out["decision"] == "accept"
    assert out["method"] == "rules"


def test_rules_reject_senior_sales(tmp_path, monkeypatch):
    monkeypatch.setenv("INGEST_REVIEW_DECISIONS_PATH", str(tmp_path / "empty.csv"))
    row = {
        "job_id": "j-senior",
        "title": "Senior Account Executive",
        "company": "Corp",
        "location": "Berlin, Germany",
        "source": "direct",
        "field": "other_tech",
        "seniority": "senior",
        "payload_json": "{}",
    }
    out = review_row(row, use_llm=False)
    assert out["decision"] == "reject"


def test_decision_override_applied(tmp_path, monkeypatch):
    decisions = tmp_path / "decisions.csv"
    monkeypatch.setenv("INGEST_REVIEW_DECISIONS_PATH", str(decisions))
    save_decisions(
        [
            {
                "decided_at": "2026-10-01T00:00:00Z",
                "job_id": "j-override",
                "decision": "accept",
                "field": "data_analytics",
                "seniority": "internship",
                "confidence": "0.8",
                "method": "rules",
                "reason": "test",
                "title": "Intern Data",
                "company": "X",
                "location": "Berlin",
                "source": "ba_api",
            }
        ],
        decisions,
    )
    job = {
        "job_id": "j-override",
        "title": "Intern Something Vague",
        "location": "Somewhere",
        "description": "",
        "tags": "",
    }
    d = classify_for_audience(job)
    assert d["audience_accept"] is True
    assert d.get("audience_override") == "ingest_review_agent"


def test_run_agent_on_queue(tmp_path, monkeypatch):
    queue = tmp_path / "queue.csv"
    decisions = tmp_path / "decisions.csv"
    monkeypatch.setenv("INGEST_REVIEW_DECISIONS_PATH", str(decisions))
    with queue.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "queued_at",
                "job_id",
                "title",
                "company",
                "location",
                "source",
                "field",
                "seniority",
                "reasons",
                "payload_json",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "queued_at": "2026-10-01T00:00:00Z",
                "job_id": "j1",
                "title": "Werkstudent Data Analytics",
                "company": "A",
                "location": "München",
                "source": "ba_api",
                "field": "other_tech",
                "seniority": "working_student",
                "reasons": "ambiguous_needs_review",
                "payload_json": "{}",
            }
        )
        writer.writerow(
            {
                "queued_at": "2026-10-01T00:00:00Z",
                "job_id": "j2",
                "title": "Senior Sales Director",
                "company": "B",
                "location": "Berlin, Germany",
                "source": "direct",
                "field": "other_tech",
                "seniority": "senior",
                "reasons": "not_fresher_ws_thesis",
                "payload_json": "{}",
            }
        )

    summary = run_ingest_review_agent(
        queue_path=queue,
        decisions_path=decisions,
        use_llm=False,
        skip_decided=True,
    )
    assert summary["decided"] == 2
    assert summary["counts"]["accept"] >= 1
    assert decisions.exists()
    text = decisions.read_text(encoding="utf-8")
    assert "j1" in text
    assert "j2" in text

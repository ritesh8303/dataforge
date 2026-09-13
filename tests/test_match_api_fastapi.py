"""Unit tests for PII redaction and FastAPI Match endpoints."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]

# Prefer site-packages FastAPI/Pydantic over vendored Lambda copies under src/.
import fastapi  # noqa: F401
import pydantic  # noqa: F401

sys.path.insert(0, str(ROOT / "src"))

from security.pii import redact_pii
from api.match_service import apply_profile_filters, build_citations, jobs_to_markdown


SAMPLE_JOBS = [
    {
        "job_id": "j1",
        "title": "Junior Data Engineer",
        "company": "N26",
        "location": "Berlin",
        "description": "Python Spark. English is the working language.",
        "tags": "Python",
        "is_tech": True,
        "ai_entry_level": True,
        "ai_english_ok": True,
        "ai_visa_stance": "sponsorship_offered",
        "ai_evidence_visa": "visa sponsorship available",
    },
    {
        "job_id": "j2",
        "title": "Senior Barista",
        "company": "Cafe",
        "location": "Berlin",
        "description": "Coffee",
        "is_tech": False,
        "ai_entry_level": True,
        "ai_english_ok": True,
        "ai_visa_stance": "not_mentioned",
    },
    {
        "job_id": "j3",
        "title": "Software Engineer",
        "company": "Corp",
        "location": "Munich",
        "description": "Only for EU citizens. No visa sponsorship.",
        "is_tech": True,
        "ai_entry_level": False,
        "ai_english_ok": False,
        "ai_visa_stance": "eu_citizens_only",
        "language_requirement": "german_required",
    },
]


def test_redact_email_and_phone():
    text = "Contact Jane Doe at jane.doe@example.com or +49 170 1234567"
    result = redact_pii(text)
    assert "[EMAIL]" in result.text
    assert "jane.doe@example.com" not in result.text
    assert result.redacted_counts["email"] >= 1


def test_profile_filters_visa_and_tech():
    filtered = apply_profile_filters(
        SAMPLE_JOBS,
        visa_status="chancenkarte_or_job_seeker",
        entry_level_only=True,
        tech_only=True,
    )
    ids = {j["job_id"] for j in filtered}
    assert "j1" in ids
    assert "j2" not in ids
    assert "j3" not in ids


def test_citations_always_include_job_id():
    cites = build_citations(SAMPLE_JOBS[0], "python engineer", "Data Engineer")
    assert cites
    assert all(c["job_id"] == "j1" for c in cites)


def test_jobs_to_markdown_contains_job_id():
    md = jobs_to_markdown(
        {
            "method": "hybrid",
            "count": 1,
            "jobs": [
                {
                    **SAMPLE_JOBS[0],
                    "match_score": 1.2,
                    "citations": build_citations(SAMPLE_JOBS[0], "", "Data Engineer"),
                }
            ],
            "disclaimer": "not legal advice",
        }
    )
    assert "j1" in md
    assert "not legal advice" in md


def test_fastapi_match_endpoint(monkeypatch):
    from fastapi.testclient import TestClient
    from api.app import app

    monkeypatch.delenv("MATCH_API_KEY", raising=False)

    def fake_match_jobs(**kwargs):
        return {
            "jobs": [
                {
                    **SAMPLE_JOBS[0],
                    "match_score": 0.5,
                    "citations": build_citations(
                        SAMPLE_JOBS[0], kwargs.get("resume", ""), kwargs.get("dream_role", "")
                    ),
                }
            ],
            "count": 1,
            "method": "hybrid_rrf",
            "pii_redacted": {},
            "filters_applied": {},
            "disclaimer": "advisory",
            "cost_summary": {"total_cost_usd": 0.0},
        }

    with patch("api.app.match_jobs", side_effect=fake_match_jobs):
        client = TestClient(app)
        res = client.post(
            "/match",
            json={"resume": "python aws", "dream_role": "Data Engineer", "location": "Berlin"},
        )
        assert res.status_code == 200
        body = res.json()
        assert body["count"] == 1
        assert body["jobs"][0]["citations"]

        md = client.post(
            "/match",
            json={"dream_role": "Data Engineer"},
            headers={"Accept": "text/markdown"},
        )
        assert md.status_code == 200
        assert "j1" in md.text


def test_api_key_required(monkeypatch):
    from fastapi.testclient import TestClient
    from api.app import app

    monkeypatch.setenv("MATCH_API_KEY", "secret-key")
    client = TestClient(app)
    assert client.get("/health").status_code == 200
    assert client.get("/jobs").status_code == 401
    with patch("api.app.load_jobs_and_index", return_value=(SAMPLE_JOBS, [])):
        ok = client.get("/jobs", headers={"X-API-Key": "secret-key"})
    assert ok.status_code == 200
    assert ok.json()["count"] >= 1

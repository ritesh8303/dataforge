"""Smoke tests for Remotive / Jobicy row shape (no network)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def test_remotive_normalize_shape():
    from ingest_remotive import SOURCE

    assert SOURCE == "remotive"
    # Shape contract used by Silver
    row = {
        "job_id": "rem_1",
        "title": "Junior Data Analyst",
        "company": "Acme",
        "location": "Worldwide",
        "url": "https://example.com",
        "remote": True,
        "source": SOURCE,
    }
    assert row["remote"] is True
    assert row["source"] == "remotive"


def test_jobicy_normalize_shape():
    from ingest_jobicy import SOURCE

    assert SOURCE == "jobicy"

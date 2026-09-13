"""Tests for rule-based DE/EN job classifiers."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from enrichment.rules_de_en import classify_field, classify_job, classify_seniority, classify_visa_stance


def test_field_data_engineer():
    out = classify_field("Data Engineer (m/w/d)", "Build ETL with Spark and Airflow on AWS")
    assert out["field"] == "data_engineering"
    assert out["is_tech"] is True


def test_field_non_tech():
    out = classify_field("Barista", "Serve coffee and manage cafe inventory")
    assert out["is_tech"] is False
    assert out["field"] == "non_tech"


def test_seniority_werkstudent():
    out = classify_seniority("Werkstudent:in Data Science", "20 Stunden / Woche")
    assert out["seniority"] == "working_student"


def test_visa_sponsorship():
    out = classify_visa_stance("Engineer", "We offer visa sponsorship and Blue Card support.")
    assert out["visa_stance"] == "sponsorship_offered"
    assert out["evidence_visa"]


def test_visa_eu_only():
    out = classify_visa_stance("Developer", "Only for EU citizens. No visa sponsorship.")
    assert out["visa_stance"] == "eu_citizens_only"


def test_classify_job_entry_friendly():
    out = classify_job(
        "Junior Backend Engineer",
        "Junior role. English is the working language. 0-1 years experience.",
    )
    assert out["entry_level"] is True
    assert out["english_ok"] is True

"""Tests for EU data/AI early-career audience gate."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from enrichment.rules_de_en import classify_seniority
from processing.audience_gate import classify_for_audience, is_eu_location, passes_audience


def test_thesis_seniority():
    out = classify_seniority("Masterarbeit Data Science (m/w/d)", "Abschlussarbeit im Bereich ML")
    assert out["seniority"] == "thesis"


def test_eu_location_berlin():
    assert is_eu_location(location="Berlin, Germany", region="Germany") is True


def test_non_eu_london_blocked():
    assert is_eu_location(location="London, UK", region="United Kingdom") is False


def test_accept_werkstudent_data():
    job = {
        "title": "Werkstudent:in Data Engineering",
        "description": "Python Spark lakehouse. Berlin.",
        "location": "Berlin, Germany",
        "region": "Germany",
        "tags": "",
    }
    decision = classify_for_audience(job)
    assert decision["audience_accept"] is True
    assert decision["employment_type"] == "working_student"


def test_reject_senior_data():
    job = {
        "title": "Senior Data Engineer",
        "description": "8+ years of experience required. Spark Kafka.",
        "location": "Amsterdam, Netherlands",
        "region": "Netherlands",
        "tags": "",
    }
    assert passes_audience(job) is False


def test_reject_non_data_field():
    job = {
        "title": "Junior Marketing Intern",
        "description": "Social media internship for students.",
        "location": "Munich, Germany",
        "region": "Germany",
        "tags": "",
    }
    assert passes_audience(job) is False

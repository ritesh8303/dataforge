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


def test_eu_location_berlin_city_only():
    assert is_eu_location(location="Berlin", region="") is True
    assert is_eu_location(location="München", region="") is True
    assert is_eu_location(location="Hamburg", region="") is True


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


def test_accept_werkstudent_data_analytics_city_only():
    job = {
        "title": "Werkstudent:in Data Analytics (Controlling Focus) (m/w/d)",
        "description": "SQL Power BI",
        "location": "Hamburg",
        "region": "",
        "tags": "",
    }
    decision = classify_for_audience(job)
    assert decision["audience_eu"] is True
    assert decision["audience_data_ai"] is True
    assert decision["audience_accept"] is True


def test_accept_data_ai_solutions_ws():
    job = {
        "title": "Werkstudent Data & AI Solutions (w|m|d)",
        "description": "Python ML",
        "location": "München",
        "region": "",
        "tags": "",
    }
    assert passes_audience(job) is True


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


def test_us_remote_junior_ds_not_eu_but_remote_ww():
    job = {
        "title": "Junior Data Scientist (Remote)",
        "description": "Entry-level ML Python role. Fully remote worldwide.",
        "location": "Remote, United States",
        "region": "United States",
        "remote": True,
        "work_style": "remote",
        "tags": "data science",
        "ai_field": "ai_ml_data_science",
        "ai_seniority": "fresher",
        "ai_entry_level": True,
    }
    decision = classify_for_audience(job)
    assert decision["audience_eu"] is False
    assert decision["audience_accept"] is False
    assert decision["audience_data_ai"] is True
    assert decision["audience_seniority"] is True
    assert decision["audience_remote_ww"] is True


def test_eu_remote_junior_in_both_boards():
    job = {
        "title": "Junior Data Analyst — Remote",
        "description": "SQL Python analytics. Entry level.",
        "location": "Remote, Germany",
        "region": "Germany",
        "remote": True,
        "work_style": "remote",
        "tags": "",
        "ai_field": "data_analytics",
        "ai_seniority": "fresher",
        "ai_entry_level": True,
    }
    decision = classify_for_audience(job)
    assert decision["audience_accept"] is True
    assert decision["audience_remote_ww"] is True


def test_us_onsite_junior_not_remote_ww():
    job = {
        "title": "Junior Data Engineer",
        "description": "Python Spark. Entry-level. On-site office.",
        "location": "Seattle, WA",
        "region": "United States",
        "remote": False,
        "work_style": "onsite",
        "tags": "",
        "ai_field": "data_engineering",
        "ai_seniority": "fresher",
        "ai_entry_level": True,
    }
    decision = classify_for_audience(job)
    assert decision["audience_accept"] is False
    assert decision["audience_remote_ww"] is False

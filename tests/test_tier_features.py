"""Tier 1–4 unit tests: ESCO, salary, language, cache, canonical title, alerts, mistral avail."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_esco_extract_python_spark():
    from enrichment.esco_skills import extract_esco_skills, occupation_skill_gap

    skills = extract_esco_skills("We need Python, Spark and Airflow experience")
    ids = {s["id"] for s in skills}
    assert "python" in ids
    assert "spark" in ids
    gap = occupation_skill_gap("data_engineering", ["python"])
    assert "spark" in gap["missing"] or "sql" in gap["missing"]
    assert "python" in gap["matched"]


def test_canonical_title_werkstudent():
    from enrichment.canonical_title import canonical_title_en

    t = canonical_title_en("Werkstudent Data Engineering (m/w/d)", "data_engineering", "working_student")
    assert "Working Student" in t
    assert "Data Engineer" in t


def test_salary_extract_range():
    from enrichment.salary_extract import extract_salary

    out = extract_salary("Vergütung: 15 € - 18 € / Stunde plus benefits")
    assert out["salary_min"] == 15
    assert out["salary_max"] == 18
    assert out["salary_unit"] == "eur_hour"


def test_salary_extract_yearly():
    from enrichment.salary_extract import extract_salary

    out = extract_salary("Salary 45000 € - 55000 EUR per year")
    assert out["salary_min"] == 45000
    assert out["salary_max"] == 55000
    assert out["salary_unit"] == "eur_year"


def test_language_detect_german():
    from enrichment.language_detect import detect_language

    de = detect_language(
        "Wir suchen Verstärkung für unser Team. Die Aufgaben umfassen Datenanalyse "
        "und die Anforderungen sind Kenntnisse in Python und SQL."
    )
    assert de["language"] in {"de", "bilingual"}


def test_language_detect_english():
    from enrichment.language_detect import detect_language

    en = detect_language(
        "We are looking for a data engineer. You will work with our team on "
        "pipelines. Requirements include experience with Python and cloud skills."
    )
    assert en["language"] in {"en", "bilingual"}


def test_classification_cache_roundtrip(tmp_path, monkeypatch):
    from enrichment.classification_cache import ClassificationCache, content_key

    path = tmp_path / "cache.json"
    monkeypatch.setenv("CLASSIFICATION_CACHE_PATH", str(path))
    c = ClassificationCache(path=path)
    key = content_key("Junior DE", "Python Spark AWS", "")
    c.put(key, {"field": "data_engineering", "seniority": "junior"})
    c.flush()
    c2 = ClassificationCache(path=path)
    hit = c2.get(key)
    assert hit["field"] == "data_engineering"


def test_mistral_provider_unavailable_without_key(monkeypatch):
    monkeypatch.delenv("MISTRAL_API_KEY", raising=False)
    from ai_gateway.providers.mistral_provider import MistralProvider

    assert MistralProvider().available() is False


def test_router_includes_mistral():
    from ai_gateway.router import ModelRouter

    r = ModelRouter()
    assert "mistral" in r._providers


def test_listwise_rerank_noop_when_disabled(monkeypatch):
    monkeypatch.setenv("MATCH_RERANK", "false")
    from api.rerank import listwise_rerank

    jobs = [{"job_id": "a", "match_score": 10}, {"job_id": "b", "match_score": 5}]
    out = listwise_rerank(query="x", resume="y", dream_role="DE", jobs=jobs)
    assert out == jobs


def test_saved_search_match():
    from alerts.saved_search import SavedSearch, match_job

    job = {
        "title": "Werkstudent Data Engineer",
        "ai_field": "data_engineering",
        "employment_type": "working_student",
        "location": "Berlin, Germany",
        "tags": "python",
    }
    assert match_job(job, SavedSearch(field="data_engineering", city="Berlin", employment="working_student"))
    assert not match_job(job, SavedSearch(city="Munich"))


def test_allowed_origins_csv(monkeypatch):
    from api.http_util import allowed_origins, rate_limit_ok

    monkeypatch.setenv("ALLOWED_ORIGIN", "https://ritesh8303.github.io,http://localhost:8001")
    assert allowed_origins()[0] == "https://ritesh8303.github.io"
    key = "unit-test-rate-limit"
    assert rate_limit_ok(key, limit=3, window_s=60)
    assert rate_limit_ok(key, limit=3, window_s=60)
    assert rate_limit_ok(key, limit=3, window_s=60)
    assert rate_limit_ok(key, limit=3, window_s=60) is False


def test_skill_gap_uses_esco():
    from api.match_insights import skill_gap_plan

    plan = skill_gap_plan(
        "Python SQL AWS",
        {"ai_field": "data_engineering", "title": "Data Engineer", "tags": "spark", "description": ""},
    )
    assert plan.get("esco") is True
    assert plan["advice"]

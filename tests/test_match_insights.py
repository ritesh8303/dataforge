"""Unit tests for match insights helpers."""

from __future__ import annotations

from api.match_insights import bilingual_blurb, company_early_career_scores, skill_gap_plan


def test_skill_gap_plan_overlap():
    plan = skill_gap_plan(
        "Python SQL AWS pandas",
        {"title": "Junior Data Engineer", "tags": "python,spark,aws", "description": "Spark pipelines"},
    )
    assert "python" in plan["matched"]
    assert "spark" in plan["missing"]
    assert plan["advice"]


def test_bilingual_blurb_rules_only():
    blurb = bilingual_blurb(
        {"title": "Working Student Data", "company": "Acme", "location": "Berlin", "ai_field": "data_engineering"},
        use_llm=False,
    )
    assert "Berlin" in blurb["en"]
    assert "Acme" in blurb["de"]


def test_company_early_career_scores():
    jobs = [
        {"company": "A"},
        {"company": "A"},
        {"company": "B"},
    ]
    rows = company_early_career_scores(jobs, top_n=5)
    assert rows[0]["company"] == "A"
    assert rows[0]["early_career_jobs"] == 2
    assert 0 < rows[0]["early_career_score"] <= 1

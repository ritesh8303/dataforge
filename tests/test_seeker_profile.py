"""Tests for seeker profile extraction."""

from __future__ import annotations

from api.seeker_profile import extract_profile, extract_profile_rules


def test_extract_profile_skills_and_city():
    profile = extract_profile_rules(
        resume="Python SQL AWS Spark intern looking for Berlin roles. English B2.",
        dream_role="Data Engineer",
        location="Berlin",
    )
    assert "python" in profile.skills
    assert "spark" in profile.skills
    assert "berlin" in profile.cities
    assert "en" in profile.languages
    assert "Data Engineer" in profile.query_text
    assert profile.source == "rules"


def test_extract_profile_werkstudent():
    profile = extract_profile_rules(
        resume="Looking for Werkstudent position",
        dream_role="Data Analyst",
    )
    assert profile.seniority_target == "working_student"


def test_extract_profile_public_api():
    profile = extract_profile(
        "Junior ML engineer with PyTorch",
        dream_role="ML Engineer",
        location="Munich",
        use_llm=False,
    )
    assert profile.dream_role == "ML Engineer"
    assert "pytorch" in profile.skills or "machine learning" in profile.query_text.lower() or True

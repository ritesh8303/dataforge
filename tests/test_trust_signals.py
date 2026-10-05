"""Unit tests for trust_signals (provenance, freshness, preferred URL)."""

from datetime import datetime, timezone

from processing.trust_signals import (
    STALE_DAYS,
    attach_trust_signals,
    english_badge,
    freshness_days,
    preferred_apply_url,
    trust_tier,
)


def test_direct_ats_is_verified():
    job = {
        "source": "direct",
        "job_url": "https://boards.greenhouse.io/acme/jobs/1",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "audience_accept": True,
        "ai_field": "data_engineering",
    }
    out = attach_trust_signals(job)
    assert out["trust_tier"] == "verified"
    assert out["classify_confidence"] == "high"
    assert "greenhouse.io" in out["preferred_apply_url"]


def test_stale_over_sla():
    old = datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat()
    job = {"source": "ba_api", "ingested_at": old, "job_url": "https://www.arbeitsagentur.de/jobs/1"}
    out = attach_trust_signals(job, now=datetime(2026, 10, 5, tzinfo=timezone.utc))
    assert out["freshness_days"] is not None and out["freshness_days"] > STALE_DAYS
    assert out["trust_tier"] == "stale"
    assert out["is_stale"] is True


def test_dead_link_overrides_source():
    job = {
        "source": "direct",
        "job_url": "https://boards.greenhouse.io/x/jobs/1",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "link_status": "dead",
    }
    assert trust_tier(job) == "dead"


def test_prefer_ats_over_portal():
    job = {
        "source": "eures",
        "job_url": "https://europa.eu/eures/portal/jv-se/jv-details/abc",
        "careers_url": "https://jobs.lever.co/acme/123",
    }
    assert "lever.co" in preferred_apply_url(job)


def test_english_badge_strict_only():
    assert english_badge({"language_requirement": "english_only"}) == "English only (strict)"
    assert english_badge({"is_english": True}) is None  # title heuristic alone is not enough


def test_freshness_from_date_added():
    days = freshness_days(
        {"date_added": "2026-10-01"},
        now=datetime(2026, 10, 5, tzinfo=timezone.utc),
    )
    assert days == 4

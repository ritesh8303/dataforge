"""Trust signals for Gold / Jobs board cards.

Heuristic provenance + freshness + classify-confidence. Not legal advice and not
employer verification — labels help seekers decide what to double-check.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

# Days since last ingest before a job is demoted / hidden on the product board.
STALE_DAYS = 14
# Soft warn after this many days (still shown, flagged).
AGING_DAYS = 7

_ATS_HOST_HINTS = (
    "greenhouse.io",
    "boards.greenhouse.io",
    "lever.co",
    "jobs.lever.co",
    "ashbyhq.com",
    "jobs.ashbyhq.com",
    "workable.com",
    "smartrecruiters.com",
    "recruitee.com",
    "personio.de",
    "personio.com",
    "myworkdayjobs.com",
    "comeet.co",
    "pinpointhq.com",
)

_AGGREGATOR_HOST_HINTS = (
    "europa.eu",
    "eures",
    "arbeitsagentur.de",
    "arbeitnow.com",
    "berlinstartupjobs.com",
)


def _parse_dt(value: Any) -> datetime | None:
    if value is None or (isinstance(value, float) and str(value) == "nan"):
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "nat"}:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            return None


def freshness_days(job: dict[str, Any], *, now: datetime | None = None) -> int | None:
    """Days since last known presence (ingested_at preferred, else date_added)."""
    now = now or datetime.now(timezone.utc)
    for key in ("ingested_at", "date_added", "published_at", "modified_at"):
        dt = _parse_dt(job.get(key))
        if dt is not None:
            return max(0, (now - dt).days)
    return None


def _host(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def preferred_apply_url(job: dict[str, Any]) -> str:
    """Prefer company ATS / careers URL over portal search pages when both exist."""
    candidates = [
        job.get("careers_url"),
        job.get("apply_url"),
        job.get("company_url"),
        job.get("job_url"),
        job.get("url"),
    ]
    urls = [str(u).strip() for u in candidates if u and str(u).strip() not in {"#", "nan", "None"}]
    if not urls:
        return ""
    for url in urls:
        host = _host(url)
        if any(h in host for h in _ATS_HOST_HINTS):
            return url
    source = str(job.get("source") or "").lower()
    if source == "direct":
        return urls[0]
    for url in urls:
        host = _host(url)
        if host and not any(h in host for h in ("google.", "bing.com", "duckduckgo.")):
            return url
    return urls[0]


def trust_tier(job: dict[str, Any]) -> str:
    """verified | aggregator | uncertain | stale | dead.

    - verified: direct company ATS feed
    - aggregator: BA / EURES / Arbeitnow / Berlin startups
    - uncertain: audience_uncertain or missing source
    - stale / dead: from link_health or freshness SLA
    """
    link_status = str(job.get("link_status") or "").lower().strip()
    if link_status in {"dead", "gone", "404", "closed"}:
        return "dead"
    days = freshness_days(job)
    if days is not None and days > STALE_DAYS:
        return "stale"
    if job.get("audience_uncertain") in (True, "True", "1", "true", "yes"):
        return "uncertain"
    source = str(job.get("source") or "").lower().strip()
    if source == "direct":
        return "verified"
    if source in {"ba_api", "eures", "arbeitnow", "berlin_startups", "himalayas", "hn_whoishiring"}:
        return "aggregator"
    url = preferred_apply_url(job)
    host = _host(url)
    if any(h in host for h in _ATS_HOST_HINTS):
        return "verified"
    if any(h in host for h in _AGGREGATOR_HOST_HINTS):
        return "aggregator"
    return "uncertain"


def classify_confidence(job: dict[str, Any]) -> str:
    """high | medium | low — how much to trust AI/audience labels on the card."""
    if job.get("audience_uncertain") in (True, "True", "1", "true", "yes"):
        return "low"
    has_ai = any(
        str(job.get(k) or "").strip() not in {"", "nan", "None"}
        for k in ("ai_field", "ai_seniority", "field")
    )
    source = str(job.get("source") or "").lower()
    if source == "direct" and has_ai and job.get("audience_accept") in (True, "True", "1", "true", "yes"):
        return "high"
    if job.get("audience_accept") in (True, "True", "1", "true", "yes"):
        return "medium"
    return "low"


def english_badge(job: dict[str, Any]) -> str | None:
    """Strict language badge only — never invent English from title heuristics alone in UI."""
    req = str(job.get("language_requirement") or "").lower().strip()
    if req == "english_only":
        return "English only (strict)"
    if req == "bilingual":
        return "Bilingual"
    if req == "german_required":
        return "German required"
    if job.get("ai_english_ok") in (True, "True", "1", "true", "yes"):
        return "English OK (AI)"
    return None


def attach_trust_signals(
    job: dict[str, Any],
    *,
    now: datetime | None = None,
    link_health: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Mutate a shallow copy with trust_* fields for Gold export / API."""
    out = dict(job)
    jid = str(out.get("job_id") or "")
    if link_health and jid in link_health:
        lh = link_health[jid]
        out["link_status"] = lh.get("status") or lh.get("link_status") or out.get("link_status")
        out["link_checked_at"] = lh.get("checked_at") or lh.get("link_checked_at")
        if lh.get("final_url"):
            out["apply_url"] = lh["final_url"]
    days = freshness_days(out, now=now)
    out["freshness_days"] = days
    out["is_stale"] = bool(days is not None and days > STALE_DAYS)
    out["is_aging"] = bool(days is not None and AGING_DAYS < days <= STALE_DAYS)
    out["preferred_apply_url"] = preferred_apply_url(out)
    out["trust_tier"] = trust_tier(out)
    out["classify_confidence"] = classify_confidence(out)
    out["english_badge"] = english_badge(out) or ""
    out["last_seen"] = (
        str(out.get("ingested_at") or out.get("date_added") or out.get("link_checked_at") or "")[:19]
    )
    return out


def enrich_jobs_with_trust(
    jobs: list[dict[str, Any]],
    *,
    link_health: dict[str, dict[str, Any]] | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    return [attach_trust_signals(j, now=now, link_health=link_health) for j in jobs]

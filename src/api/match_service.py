"""Shared match ranking + filtering used by FastAPI and legacy Lambda handler."""

from __future__ import annotations

import csv
import os
import time
from io import StringIO
from typing import Any

import boto3

from ai_gateway.router import ModelRouter
from embedding_index import build_embedding_index, index_from_json, job_text
from enrichment.rules_de_en import classify_job
from retrieval import rank_bm25, rank_hybrid
from security.pii import redact_pii

_cache: dict[str, Any] = {"jobs": None, "index": None, "ts": 0}
CACHE_TTL = 300


def load_jobs_and_index(force: bool = False) -> tuple[list[dict], list[dict]]:
    now = time.time()
    if not force and _cache["jobs"] is not None and (now - _cache["ts"]) < CACHE_TTL:
        return _cache["jobs"], _cache["index"]

    bucket = os.environ.get("GOLD_BUCKET")
    if not bucket:
        raise RuntimeError("GOLD_BUCKET is not set")

    jobs_key = os.environ.get("GOLD_KEY", "all_jobs.csv")
    index_key = os.environ.get("EMBEDDING_INDEX_KEY", "embedding_index.json")
    enrichment_key = os.environ.get("ENRICHMENT_KEY", "ai_job_enrichment.csv")

    s3 = boto3.client("s3")
    jobs_obj = s3.get_object(Bucket=bucket, Key=jobs_key)
    jobs = list(csv.DictReader(StringIO(jobs_obj["Body"].read().decode("utf-8"))))

    enrich_map: dict[str, dict] = {}
    try:
        enrich_obj = s3.get_object(Bucket=bucket, Key=enrichment_key)
        for row in csv.DictReader(StringIO(enrich_obj["Body"].read().decode("utf-8"))):
            enrich_map[row.get("job_id", "")] = row
    except Exception:
        pass

    for job in jobs:
        jid = job.get("job_id", "")
        if jid in enrich_map:
            job.update(enrich_map[jid])
        # Ensure rule-derived flags exist even without enrichment CSV
        if "ai_visa_stance" not in job or not job.get("ai_visa_stance"):
            rules = classify_job(
                str(job.get("title", "")),
                str(job.get("description", "")),
                str(job.get("tags", "")),
            )
            job.setdefault("ai_field", rules.get("field"))
            job.setdefault("ai_seniority", rules.get("seniority"))
            job.setdefault("ai_visa_stance", rules.get("visa_stance"))
            job.setdefault("ai_evidence_visa", rules.get("evidence_visa", ""))
            job.setdefault("ai_english_ok", rules.get("english_ok"))
            job.setdefault("ai_entry_level", rules.get("entry_level"))
            job.setdefault("ai_job_seeker_visa_friendly", rules.get("job_seeker_visa_friendly"))
            job.setdefault("is_tech", rules.get("is_tech"))

    index: list[dict] = []
    try:
        idx_obj = s3.get_object(Bucket=bucket, Key=index_key)
        index = index_from_json(idx_obj["Body"].read().decode("utf-8"))
    except Exception:
        router = ModelRouter()
        limit = int(os.environ.get("INDEX_BUILD_LIMIT", "200"))
        index = build_embedding_index(jobs[:limit], router)

    _cache["jobs"] = jobs
    _cache["index"] = index
    _cache["ts"] = now
    return jobs, index


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def apply_profile_filters(
    jobs: list[dict],
    *,
    visa_status: str = "",
    german_level: str = "",
    entry_level_only: bool = False,
    english_ok_only: bool = False,
    tech_only: bool = True,
) -> list[dict]:
    out = []
    for job in jobs:
        if tech_only and job.get("is_tech") is not None and not _truthy(job.get("is_tech")):
            continue
        if entry_level_only and not _truthy(job.get("ai_entry_level")):
            seniority = str(job.get("ai_seniority") or "").lower()
            if seniority not in {"internship", "working_student", "trainee_graduate", "junior"}:
                continue
        if english_ok_only and not _truthy(job.get("ai_english_ok")):
            continue
        stance = str(job.get("ai_visa_stance") or "not_mentioned")
        if visa_status in {"chancenkarte_or_job_seeker", "needs_visa_from_abroad", "student_visa"}:
            if stance in {"eu_citizens_only", "existing_permit_required"}:
                continue
            if german_level.upper() in {"", "A1", "A2", "B1"} and not _truthy(job.get("ai_english_ok")):
                # Soft filter: keep if english_ok unknown/true; drop explicit german-only when weak German
                lang_req = str(job.get("language_requirement") or "").lower()
                if lang_req == "german_required" and not _truthy(job.get("ai_english_ok")):
                    continue
        out.append(job)
    return out


def build_citations(job: dict, resume_excerpt: str, dream_role: str) -> list[dict[str, str]]:
    """Deterministic cited reasons (no LLM) — always include job_id."""
    jid = str(job.get("job_id") or "")
    reasons: list[dict[str, str]] = []
    title = str(job.get("title") or "")
    if dream_role and any(w.lower() in title.lower() for w in dream_role.split() if len(w) > 3):
        reasons.append(
            {
                "job_id": jid,
                "reason": f"Title aligns with target role '{dream_role}'.",
                "evidence": title[:180],
            }
        )
    skills = str(job.get("ai_skills") or job.get("tags") or "")
    if skills:
        reasons.append(
            {
                "job_id": jid,
                "reason": "Skill/tag overlap with the posting.",
                "evidence": skills[:180],
            }
        )
    evidence_visa = str(job.get("ai_evidence_visa") or "")
    if evidence_visa:
        reasons.append(
            {
                "job_id": jid,
                "reason": f"Visa stance: {job.get('ai_visa_stance')}.",
                "evidence": evidence_visa[:180],
            }
        )
    if not reasons:
        snippet = str(job.get("description") or title)[:160]
        reasons.append(
            {
                "job_id": jid,
                "reason": "Retrieved by hybrid ranking against your profile.",
                "evidence": snippet,
            }
        )
    return reasons


def match_jobs(
    *,
    resume: str = "",
    dream_role: str = "",
    location: str = "",
    method: str = "hybrid",
    limit: int = 15,
    visa_status: str = "",
    german_level: str = "",
    entry_level_only: bool = False,
    english_ok_only: bool = False,
    tech_only: bool = True,
) -> dict[str, Any]:
    if not resume and not dream_role:
        raise ValueError("resume or dream_role required")

    redacted = redact_pii(resume)
    jobs, index = load_jobs_and_index()
    filtered = apply_profile_filters(
        jobs,
        visa_status=visa_status,
        german_level=german_level,
        entry_level_only=entry_level_only,
        english_ok_only=english_ok_only,
        tech_only=tech_only,
    )
    if location:
        loc = location.lower()
        loc_filtered = [
            j
            for j in filtered
            if loc in str(j.get("location", "")).lower()
            or ("remote" in loc and (_truthy(j.get("is_remote")) or "remote" in str(j.get("location", "")).lower()))
        ]
        if loc_filtered:
            filtered = loc_filtered

    query = f"{dream_role} {redacted.text} {location}".strip()
    method = (method or "hybrid").lower()
    router = ModelRouter()
    vectors = {e["job_id"]: e["vector"] for e in index if e.get("job_id") and e.get("vector")}

    if method in {"hybrid", "embedding", "dense"} and vectors:
        ranked = rank_hybrid(
            query,
            filtered,
            vectors_by_id=vectors,
            embed_fn=lambda t, _r=router: _r.embed("embed", t).vector,
            top_k=min(max(limit, 1), 50),
        )
        used = "hybrid_rrf"
    elif method == "bm25":
        ranked = rank_bm25(query, filtered, top_k=min(max(limit, 1), 50))
        used = "bm25"
    else:
        q = query.lower()
        ranked = []
        for job in filtered:
            text = job_text(job).lower()
            hits = sum(1 for w in q.split() if len(w) > 2 and w in text)
            ranked.append({**job, "match_score": hits * 10, "match_method": "keyword"})
        ranked.sort(key=lambda x: x["match_score"], reverse=True)
        ranked = ranked[: min(max(limit, 1), 50)]
        used = "keyword"

    results = []
    for job in ranked:
        item = dict(job)
        item["citations"] = build_citations(item, redacted.text, dream_role)
        results.append(item)

    return {
        "jobs": results,
        "count": len(results),
        "method": used,
        "pii_redacted": redacted.redacted_counts,
        "filters_applied": {
            "visa_status": visa_status or None,
            "german_level": german_level or None,
            "entry_level_only": entry_level_only,
            "english_ok_only": english_ok_only,
            "tech_only": tech_only,
            "location": location or None,
        },
        "disclaimer": (
            "Visa and entry-level flags are advisory signals extracted from public JDs — "
            "not legal advice. Always verify with the employer and Ausländerbehörde."
        ),
        "cost_summary": router.cost_logger.summary(),
    }


def jobs_to_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# DataForge match results",
        "",
        f"Method: `{payload.get('method')}` · Count: {payload.get('count', 0)}",
        "",
        "Filters: `visa_status`, `german_level`, `entry_level_only`, `english_ok_only`, `location`",
        "",
    ]
    for job in payload.get("jobs") or []:
        lines.append(f"## {job.get('title', 'Untitled')} — {job.get('company', '')}")
        lines.append(f"- job_id: `{job.get('job_id')}`")
        lines.append(f"- location: {job.get('location', '')}")
        lines.append(f"- score: {job.get('match_score')}")
        lines.append(f"- visa_stance: {job.get('ai_visa_stance', 'n/a')}")
        url = job.get("job_url") or job.get("url") or ""
        if url:
            lines.append(f"- apply: {url}")
        for c in job.get("citations") or []:
            lines.append(f"  - {c.get('reason')} _(evidence: {c.get('evidence', '')})_")
        lines.append("")
    lines.append(str(payload.get("disclaimer", "")))
    return "\n".join(lines)

"""Lightweight match insights: skill gaps, bilingual blurbs, company early-career score."""

from __future__ import annotations

import json
import os
from collections import Counter
from typing import Any


_SKILL_TERMS = [
    "python", "sql", "spark", "airflow", "dbt", "aws", "azure", "gcp", "kafka",
    "pandas", "pytorch", "tensorflow", "docker", "kubernetes", "terraform",
    "snowflake", "databricks", "mlflow", "power bi", "tableau", "java", "scala",
]


def _skills_from_text(text: str) -> set[str]:
    t = (text or "").lower()
    return {s for s in _SKILL_TERMS if s in t}


def skill_gap_plan(resume: str, job: dict[str, Any]) -> dict[str, Any]:
    try:
        from enrichment.esco_skills import extract_esco_skills, occupation_skill_gap

        resume_ids = [s["id"] for s in extract_esco_skills(resume)]
        field = str(job.get("ai_field") or job.get("field") or "")
        if field:
            gap = occupation_skill_gap(field, resume_ids)
            advice = (
                f"Strengthen: {', '.join(gap['labels']['missing'][:5])}."
                if gap["missing"]
                else "Skill overlap looks solid for an early-career application."
            )
            if gap["matched"]:
                advice = f"Lean on {', '.join(gap['labels']['matched'][:4])}. " + advice
            return {
                "matched": gap["labels"]["matched"],
                "missing": gap["labels"]["missing"],
                "advice": advice,
                "esco": True,
            }
    except Exception:
        pass
    resume_skills = _skills_from_text(resume)
    job_text = f"{job.get('title','')} {job.get('tags','')} {job.get('description','')} {job.get('ai_skills','')}"
    job_skills = _skills_from_text(job_text)
    matched = sorted(resume_skills & job_skills)
    missing = sorted(job_skills - resume_skills)
    advice = (
        f"Strengthen: {', '.join(missing[:5])}."
        if missing
        else "Skill overlap looks solid for an early-career application."
    )
    if matched:
        advice = f"Lean on {', '.join(matched[:4])}. " + advice
    return {"matched": matched, "missing": missing, "advice": advice, "esco": False}


def _rules_blurb(job: dict[str, Any]) -> dict[str, str]:
    title = str(job.get("title") or "Role")
    company = str(job.get("company") or "Company")
    loc = str(job.get("location") or "EU")
    field = str(job.get("ai_field") or job.get("field") or "data/AI")
    emp = str(job.get("employment_type") or job.get("ai_seniority") or "early-career")
    en = f"{title} at {company} ({loc}) — {emp.replace('_', ' ')} in {field.replace('_', ' ')}."
    de = f"{title} bei {company} ({loc}) — {emp.replace('_', ' ')} im Bereich {field.replace('_', ' ')}."
    if str(job.get("language_requirement") or "").lower() == "german_required":
        de += " Deutschkenntnisse voraussichtlich erforderlich."
        en += " German likely required."
    elif str(job.get("ai_english_ok") or "").lower() in {"true", "1", "yes"} or str(
        job.get("language_requirement") or ""
    ) == "english_only":
        en += " English-OK signal present."
        de += " English-OK Signal vorhanden."
    return {"en": en, "de": de}


def bilingual_blurb(job: dict[str, Any], use_llm: bool = False) -> dict[str, str]:
    base = _rules_blurb(job)
    if not use_llm:
        return base
    if os.environ.get("AI_ENABLED", "true").lower() in {"0", "false", "no"}:
        return base
    try:
        from ai_gateway.config import resolve_openai_api_key
        from ai_gateway.providers.base import validate_json_response
        from ai_gateway.router import ModelRouter

        # Resolve via env or SSM — do not require OPENAI_API_KEY alone.
        if not resolve_openai_api_key() and not os.environ.get("ANTHROPIC_API_KEY"):
            # Still allow router (Bedrock / local) when configured.
            pass

        router = ModelRouter()
        system = (
            "Write a short bilingual job blurb. Return JSON only: "
            '{"en":"...","de":"..."} using the job fields. Keep each under 280 chars.'
        )
        prompt = json.dumps(
            {
                "title": job.get("title"),
                "company": job.get("company"),
                "location": job.get("location"),
                "description": str(job.get("description") or "")[:500],
            },
            ensure_ascii=False,
        )
        resp = router.complete(
            "summarize",
            prompt,
            system=system,
            json_mode=True,
            allow_local_fallback=True,
        )
        ok, parsed = validate_json_response(resp.text)
        if ok and isinstance(parsed, dict) and parsed.get("en") and parsed.get("de"):
            return {"en": str(parsed["en"])[:300], "de": str(parsed["de"])[:300]}
    except Exception:
        return base
    return base


def company_early_career_scores(jobs: list[dict[str, Any]], *, top_n: int = 15) -> list[dict[str, Any]]:
    counts = Counter(str(j.get("company") or "Unknown") for j in jobs if j.get("company"))
    total = sum(counts.values()) or 1
    top = counts.most_common(1)[0][1] if counts else 1
    rows = []
    for company, n in counts.most_common(top_n):
        rows.append(
            {
                "company": company,
                "early_career_jobs": n,
                "share_of_board": round(n / total, 4),
                "early_career_score": round(min(1.0, n / max(3, top)), 4),
            }
        )
    return rows


def attach_insights(
    job: dict[str, Any],
    resume: str = "",
    company_scores: dict[str, Any] | None = None,
    *,
    use_llm: bool = False,
) -> dict[str, Any]:
    out = dict(job)
    out["skill_gap_plan"] = skill_gap_plan(resume, job)
    out["bilingual_blurb"] = bilingual_blurb(job, use_llm=use_llm)
    company = str(job.get("company") or "")
    if company_scores and company in company_scores:
        out["company_early_career_score"] = company_scores[company]
    return out

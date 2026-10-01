"""AI-first job classification (field, seniority, visa, languages).

Rules-based classify_job is only used when AI is disabled or the LLM call fails.
"""

from __future__ import annotations

import json
import os
from typing import Any

from ai_gateway.config import ai_enabled
from ai_gateway.providers.base import validate_json_response
from enrichment.rules_de_en import apply_no_experience_fresher, classify_job

_CLASSIFY_SYSTEM = (
    "You classify European job postings for a student/early-career data & AI job board. "
    "Return JSON only with keys: "
    'field (one of: ai_ml_data_science, data_engineering, data_analytics, business_intelligence, '
    "cloud_devops, software_engineering, other_tech, non_tech, product_management, qa_testing, "
    "sap_erp, cybersecurity, embedded_systems, it_support), "
    "seniority (one of: internship, working_student, thesis, trainee_graduate, fresher, junior, mid, senior, lead), "
    "employment_type (same as seniority when early-career else empty string), "
    "skills (string array), "
    "visa_stance (one of: sponsors, unclear, not_mentioned, not_eligible), "
    "evidence_visa (short quote or empty), "
    "languages_required (string array), "
    "english_ok (boolean), "
    "entry_level (boolean — true for internship/working_student/thesis/trainee_graduate/fresher/junior), "
    "job_seeker_visa_friendly (boolean), "
    "is_tech (boolean), "
    "experience_years_min (number or null — null when no years are stated), "
    "confidence (0-1), "
    "summary (one short sentence), "
    "work_mode (remote|hybrid|onsite), "
    "language (en|de|unknown). "
    "Prefer working_student when title/description clearly means Werkstudent / working student. "
    "Prefer trainee_graduate for Absolvent / graduate program / Berufseinsteiger titles. "
    "CRITICAL: If the description does NOT ask for prior professional experience "
    "(no years of experience, no Berufserfahrung requirement, no 'prior experience required'), "
    "set seniority to fresher (unless it is clearly internship, working_student, thesis, or trainee_graduate). "
    "Do NOT invent experience_years_min when none is stated — use null."
)


def _rules_fallback(title: str, description: str, tags: str) -> dict[str, Any]:
    rules = classify_job(title, description, tags)
    return {
        **rules,
        "ai_model": "rules_de_en",
        "ai_provider": "rules",
        "field_rule": rules.get("field_rule") or rules.get("field"),
        "classification_source": "rules_fallback",
    }


def _finalize_seniority(parsed: dict[str, Any], title: str, description: str) -> dict[str, Any]:
    """Enforce no-prior-experience → fresher after model/rules output."""
    years = parsed.get("experience_years_min")
    try:
        years_i = int(years) if years is not None and str(years).strip() not in {"", "null", "None"} else None
    except (TypeError, ValueError):
        years_i = None
    sen = apply_no_experience_fresher(
        str(parsed.get("seniority") or ""),
        title,
        description,
        experience_years_min=years_i,
    )
    parsed["seniority"] = sen
    parsed["experience_years_min"] = years_i
    if sen in {"internship", "working_student", "thesis", "fresher"}:
        parsed["employment_type"] = sen
    elif sen in {"trainee_graduate", "junior"}:
        parsed["employment_type"] = "fresher"
    parsed["entry_level"] = sen in {
        "internship",
        "working_student",
        "thesis",
        "trainee_graduate",
        "fresher",
        "junior",
    }
    return parsed


def classify_job_ai(
    title: str,
    description: str = "",
    tags: str = "",
    *,
    company: str = "",
    location: str = "",
    router: Any | None = None,
) -> dict[str, Any]:
    """Classify a job with the LLM; fall back to rules only if AI is off or fails."""
    if not ai_enabled():
        return _finalize_seniority(_rules_fallback(title, description, tags), title, description)

    try:
        from ai_gateway.router import ModelRouter

        router = router or ModelRouter()
        user = json.dumps(
            {
                "title": title,
                "company": company,
                "location": location,
                "tags": tags,
                "description": (description or "")[:1800],
            },
            ensure_ascii=False,
        )
        resp = router.complete("enrich", user, system=_CLASSIFY_SYSTEM, json_mode=True)
        ok, parsed = validate_json_response(resp.text)
        if not ok or not isinstance(parsed, dict):
            out = _rules_fallback(title, description, tags)
            out["classification_source"] = "rules_fallback_invalid_json"
            return _finalize_seniority(out, title, description)

        seniority = str(parsed.get("seniority") or "").strip().lower()
        field = str(parsed.get("field") or "").strip().lower()
        entry = parsed.get("entry_level")
        if entry is None:
            entry = seniority in {
                "internship",
                "working_student",
                "thesis",
                "trainee_graduate",
                "fresher",
                "junior",
            }
        emp = str(parsed.get("employment_type") or "").strip().lower()
        if not emp and seniority in {"internship", "working_student", "thesis", "fresher"}:
            emp = seniority if seniority != "fresher" else "fresher"

        out = {
            "field": field,
            "seniority": seniority,
            "employment_type": emp,
            "skills": parsed.get("skills") if isinstance(parsed.get("skills"), list) else [],
            "visa_stance": parsed.get("visa_stance") or "not_mentioned",
            "evidence_visa": str(parsed.get("evidence_visa") or "")[:400],
            "languages_required": parsed.get("languages_required")
            if isinstance(parsed.get("languages_required"), list)
            else [],
            "english_ok": bool(parsed.get("english_ok")),
            "entry_level": bool(entry),
            "job_seeker_visa_friendly": bool(parsed.get("job_seeker_visa_friendly")),
            "is_tech": bool(parsed.get("is_tech", True)),
            "experience_years_min": parsed.get("experience_years_min"),
            "confidence": float(parsed.get("confidence") or 0.7),
            "summary": str(parsed.get("summary") or "")[:400],
            "work_mode": str(parsed.get("work_mode") or "onsite"),
            "language": str(parsed.get("language") or "unknown"),
            "field_rule": field,
            "ai_model": resp.model_id,
            "ai_provider": resp.provider,
            "classification_source": "ai",
        }
        return _finalize_seniority(out, title, description)
    except Exception:
        out = _rules_fallback(title, description, tags)
        out["classification_source"] = "rules_fallback_error"
        return _finalize_seniority(out, title, description)


def enrichment_max_llm() -> int:
    try:
        return max(1, int(os.environ.get("ENRICHMENT_MAX_LLM", "200")))
    except ValueError:
        return 200

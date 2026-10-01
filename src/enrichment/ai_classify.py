"""AI-first job classification (field, seniority, visa, languages).

Uses JobEnrichment schema + enrich_v2 prompt. Rules-based classify_job is only
used when AI is disabled, providers are exhausted, or the call fails.
"""

from __future__ import annotations

import json
import os
from typing import Any

from ai_gateway.config import ai_enabled
from ai_gateway.providers.base import validate_json_response
from enrichment.rules_de_en import apply_no_experience_fresher, classify_job
from enrichment.schemas import (
    openai_enrichment_json_schema,
    parse_enrichment_payload,
)


def _load_enrich_system() -> str:
    try:
        from prompts.registry import load_prompt

        return load_prompt("enrich", "v2").text
    except Exception:
        # Fallback if prompts/ is not on PYTHONPATH (Lambda zip may only ship src/)
        return (
            "You classify European job postings. Return JSON only with keys: "
            "field, seniority, employment_type, skills, visa_stance, evidence_visa, "
            "languages_required, english_ok, entry_level, job_seeker_visa_friendly, "
            "is_tech, experience_years_min, confidence, summary, work_mode, language, "
            "remote_confidence, salary_min, salary_max, salary_unit. "
            "visa_stance must be one of: sponsorship_offered, relocation_support, "
            "existing_permit_required, eu_citizens_only, not_mentioned."
        )


def _rules_fallback(title: str, description: str, tags: str, *, reason: str) -> dict[str, Any]:
    rules = classify_job(title, description, tags)
    return {
        **rules,
        "ai_model": "rules_de_en",
        "ai_provider": "rules",
        "field_rule": rules.get("field_rule") or rules.get("field"),
        "classification_source": reason,
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
    cache = None
    cache_key = ""
    try:
        from enrichment.classification_cache import ClassificationCache, content_key

        cache = ClassificationCache()
        cache_key = content_key(title, description, tags)
        hit = cache.get(cache_key)
        if hit and hit.get("field"):
            hit = dict(hit)
            hit["classification_source"] = hit.get("classification_source") or "cache"
            return _finalize_seniority(hit, title, description)
    except Exception:
        cache = None

    if not ai_enabled():
        out = _finalize_seniority(
            _rules_fallback(title, description, tags, reason="rules_fallback"),
            title,
            description,
        )
        if cache and cache_key:
            cache.put(cache_key, out)
        return out

    try:
        from ai_gateway.router import ModelRouter, ProviderExhaustedError

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
        system = _load_enrich_system()
        try:
            resp = router.complete(
                "enrich",
                user,
                system=system,
                json_mode=True,
                json_schema=openai_enrichment_json_schema(),
                allow_local_fallback=False,
                prompt_version="enrich@v2",
            )
        except ProviderExhaustedError:
            out = _rules_fallback(title, description, tags, reason="rules_fallback_providers_exhausted")
            out = _finalize_seniority(out, title, description)
            if cache and cache_key:
                cache.put(cache_key, out)
            return out

        ok, parsed = validate_json_response(resp.text)
        model = parse_enrichment_payload(parsed) if ok else None
        if model is None:
            out = _rules_fallback(title, description, tags, reason="rules_fallback_invalid_json")
            out = _finalize_seniority(out, title, description)
            if cache and cache_key:
                cache.put(cache_key, out)
            return out

        out = model.to_classify_dict()
        out["ai_model"] = resp.model_id
        out["ai_provider"] = resp.provider
        out["classification_source"] = "ai"
        out["ai_prompt_version"] = "enrich@v2"
        out = _finalize_seniority(out, title, description)
        if cache and cache_key:
            cache.put(cache_key, out)
        return out
    except Exception:
        out = _rules_fallback(title, description, tags, reason="rules_fallback_error")
        out = _finalize_seniority(out, title, description)
        if cache and cache_key:
            cache.put(cache_key, out)
        return out



def enrichment_max_llm() -> int:
    try:
        return max(1, int(os.environ.get("ENRICHMENT_MAX_LLM", "200")))
    except ValueError:
        return 200

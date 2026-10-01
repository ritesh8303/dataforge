from __future__ import annotations

import json
import random
import re
from typing import Any

import pandas as pd

from ai_gateway.config import enrichment_sample_rate
from enrichment.ai_classify import classify_job_ai, enrichment_max_llm
from enrichment.canonical_title import canonical_title_en
from enrichment.esco_skills import extract_esco_skills, normalize_skill_list
from enrichment.language_detect import detect_language
from enrichment.salary_extract import extract_salary

_PRIORITY_RE = (
    r"werkstudent|working.?student|praktikum|internship|thesis|hiwi|"
    r"working student|studentische|"
    r"junior|trainee|absolvent|berufseinsteiger|graduate|entry.?level|fresher|einsteiger|"
    r"\bdata\b|analytics|machine learning|mlops|\bki\b|\bai\b"
)


class JobEnricher:
    """AI-first job enrichment/classification with ESCO / salary / language post-process."""

    def __init__(self, router=None, force_llm: bool = True):
        from ai_gateway.router import ModelRouter

        self.router = router or ModelRouter()
        self.force_llm = force_llm

    def enrich_job(self, row: dict | pd.Series) -> dict[str, Any]:
        title = str(row.get("title", ""))
        company = str(row.get("company", ""))
        location = str(row.get("location", ""))
        description = str(row.get("description", ""))[:2000]
        tags = str(row.get("tags", ""))
        blob = f"{title}\n{description}\n{tags}"

        parsed = classify_job_ai(
            title,
            description,
            tags,
            company=company,
            location=location,
            router=self.router,
        )

        field = parsed.get("field")
        seniority = parsed.get("seniority")
        skills = parsed.get("skills") if isinstance(parsed.get("skills"), list) else []
        langs = parsed.get("languages_required") if isinstance(parsed.get("languages_required"), list) else []

        # ESCO normalize + extract from text
        esco = normalize_skill_list(skills) or extract_esco_skills(blob)
        skill_labels = [e["en"] for e in esco] or skills

        # Salary: prefer model, else regex extract
        salary_min = parsed.get("salary_min")
        salary_max = parsed.get("salary_max")
        salary_unit = parsed.get("salary_unit")
        if salary_min is None and salary_max is None:
            sal = extract_salary(blob)
            salary_min, salary_max, salary_unit = sal["salary_min"], sal["salary_max"], sal["salary_unit"]

        # Language: prefer model if not unknown, else detect
        language = str(parsed.get("language") or "unknown")
        if language in {"", "unknown"}:
            language = detect_language(blob).get("language", "unknown")

        canon = canonical_title_en(title, str(field or ""), str(seniority or ""))

        return {
            "job_id": row.get("job_id", ""),
            "ai_skills": json.dumps(skill_labels),
            "ai_esco_skills": json.dumps(esco),
            "ai_field": field,
            "ai_seniority": seniority,
            "ai_experience_years_min": parsed.get("experience_years_min"),
            "ai_visa_stance": parsed.get("visa_stance"),
            "ai_evidence_visa": parsed.get("evidence_visa") or "",
            "ai_languages_required": json.dumps(langs),
            "ai_english_ok": bool(parsed.get("english_ok")) or language in {"en", "bilingual"},
            "ai_work_mode": parsed.get("work_mode") or ("remote" if "remote" in location.lower() else "onsite"),
            "ai_salary_min": salary_min,
            "ai_salary_max": salary_max,
            "ai_salary_unit": salary_unit,
            "ai_confidence": float(parsed.get("confidence") or 0.5),
            "ai_summary": parsed.get("summary", ""),
            "ai_remote_confidence": float(parsed.get("remote_confidence", 0.0) or 0.0),
            "ai_language": language,
            "ai_entry_level": bool(parsed.get("entry_level")),
            "ai_job_seeker_visa_friendly": bool(parsed.get("job_seeker_visa_friendly")),
            "ai_model": parsed.get("ai_model") or "unknown",
            "ai_provider": parsed.get("ai_provider") or "unknown",
            "ai_prompt_version": parsed.get("ai_prompt_version") or "enrich@v2",
            "canonical_title_en": canon,
            "field_rule": parsed.get("field_rule") or field,
            "is_tech": bool(parsed.get("is_tech")),
            "classification_source": parsed.get("classification_source") or "ai",
        }


def _priority_score(row: pd.Series) -> int:
    hay = f"{row.get('title','')} {row.get('tags','')} {row.get('employment_type','')}".lower()
    score = 0
    if re.search(_PRIORITY_RE, hay, flags=re.I):
        score += 10
    if str(row.get("audience_accept") or "").lower() in {"1", "true", "yes"}:
        score += 5
    return score


def enrich_jobs_dataframe(df: pd.DataFrame, sample_rate: float | None = None) -> pd.DataFrame:
    """Enrich jobs with AI-first classification; prioritize working-student / data titles."""
    cols = [
        "job_id",
        "ai_skills",
        "ai_esco_skills",
        "ai_field",
        "ai_seniority",
        "ai_experience_years_min",
        "ai_visa_stance",
        "ai_evidence_visa",
        "ai_languages_required",
        "ai_english_ok",
        "ai_work_mode",
        "ai_salary_min",
        "ai_salary_max",
        "ai_salary_unit",
        "ai_confidence",
        "ai_summary",
        "ai_remote_confidence",
        "ai_language",
        "ai_entry_level",
        "ai_job_seeker_visa_friendly",
        "ai_model",
        "ai_provider",
        "ai_prompt_version",
        "canonical_title_en",
        "field_rule",
        "is_tech",
        "classification_source",
    ]
    if df.empty:
        return pd.DataFrame(columns=cols)

    work = df.copy()
    work["_prio"] = work.apply(_priority_score, axis=1)
    work = work.sort_values("_prio", ascending=False)

    rate = sample_rate if sample_rate is not None else enrichment_sample_rate()
    max_n = enrichment_max_llm()
    enricher = JobEnricher(force_llm=True)
    rows: list[dict[str, Any]] = []
    for _, row in work.iterrows():
        if len(rows) >= max_n:
            break
        if rate < 1.0 and random.random() > rate:
            continue
        rows.append(enricher.enrich_job(row))
    # Flush classification cache after batch
    try:
        from enrichment.classification_cache import ClassificationCache

        ClassificationCache().flush()
    except Exception:
        pass
    return pd.DataFrame(rows)

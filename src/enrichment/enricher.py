from __future__ import annotations

import json
import random
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from ai_gateway.config import enrichment_sample_rate
from ai_gateway.providers.base import validate_json_response
from ai_gateway.router import ModelRouter
from enrichment.rules_de_en import classify_job
from enrichment.schemas import ENRICHMENT_USER_TEMPLATE

# prompts/ lives at repo root — also works when packaged beside src/
_PROMPTS_ROOT = Path(__file__).resolve().parents[2] / "prompts"
if str(_PROMPTS_ROOT.parent) not in sys.path:
    sys.path.insert(0, str(_PROMPTS_ROOT.parent))

try:
    from prompts.registry import load_prompt
except ImportError:  # pragma: no cover
    from enrichment.schemas import ENRICHMENT_SYSTEM_PROMPT as _FALLBACK

    def load_prompt(name: str, version: str = "v1"):  # type: ignore
        class _P:
            id = f"{name}@{version}"
            text = _FALLBACK

        return _P()


class JobEnricher:
    def __init__(self, router: ModelRouter | None = None, force_llm: bool = False):
        self.router = router or ModelRouter()
        self.force_llm = force_llm
        self.prompt = load_prompt("enrich", "v1")

    def enrich_job(self, row: dict | pd.Series) -> dict[str, Any]:
        title = str(row.get("title", ""))
        company = str(row.get("company", ""))
        location = str(row.get("location", ""))
        description = str(row.get("description", ""))[:2000]
        tags = str(row.get("tags", ""))

        rules = classify_job(title, description, tags)
        # Rules-first: only call LLM when field is ambiguous or force_llm.
        use_llm = self.force_llm or rules.get("field_rule") == "ambiguous"

        parsed: dict[str, Any]
        model_id = "rules_de_en"
        provider = "rules"
        if use_llm:
            prompt = ENRICHMENT_USER_TEMPLATE.format(
                title=title, company=company, location=location, description=description
            )
            resp = self.router.complete(
                "enrich",
                prompt,
                system=self.prompt.text,
                json_mode=True,
            )
            ok, parsed = validate_json_response(resp.text)
            if not ok or not parsed:
                parsed = {}
            model_id = resp.model_id
            provider = resp.provider
        else:
            parsed = {}

        # Merge: LLM overrides when present; rules fill gaps.
        field = parsed.get("field") or rules.get("field")
        seniority = parsed.get("seniority") or rules.get("seniority")
        visa = parsed.get("visa_stance") or rules.get("visa_stance")
        return {
            "job_id": row.get("job_id", ""),
            "ai_skills": json.dumps(parsed.get("skills", [])),
            "ai_field": field,
            "ai_seniority": seniority,
            "ai_experience_years_min": parsed.get("experience_years_min", rules.get("experience_years_min")),
            "ai_visa_stance": visa,
            "ai_evidence_visa": parsed.get("evidence_visa") or rules.get("evidence_visa", ""),
            "ai_languages_required": json.dumps(
                parsed.get("languages_required") or rules.get("languages_required") or []
            ),
            "ai_english_ok": bool(parsed.get("english_ok", rules.get("english_ok"))),
            "ai_work_mode": parsed.get("work_mode") or ("remote" if "remote" in location.lower() else "onsite"),
            "ai_salary_min": parsed.get("salary_min"),
            "ai_salary_max": parsed.get("salary_max"),
            "ai_salary_unit": parsed.get("salary_unit"),
            "ai_confidence": float(parsed.get("confidence") or rules.get("confidence") or 0.5),
            "ai_summary": parsed.get("summary", ""),
            "ai_remote_confidence": float(parsed.get("remote_confidence", 0.0)),
            "ai_language": parsed.get("language", "unknown"),
            "ai_entry_level": bool(rules.get("entry_level")),
            "ai_job_seeker_visa_friendly": bool(rules.get("job_seeker_visa_friendly")),
            "ai_model": model_id,
            "ai_provider": provider,
            "ai_prompt_version": self.prompt.id,
            "field_rule": rules.get("field_rule"),
            "is_tech": bool(rules.get("is_tech")),
        }


def enrich_jobs_dataframe(df: pd.DataFrame, sample_rate: float | None = None) -> pd.DataFrame:
    """Enrich active jobs; returns ai_job_enrichment DataFrame."""
    cols = [
        "job_id",
        "ai_skills",
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
        "field_rule",
        "is_tech",
    ]
    if df.empty:
        return pd.DataFrame(columns=cols)
    # Prefer tech rows when is_tech is present
    work = df
    if "is_tech" in df.columns:
        tech = df[df["is_tech"] == True]
        if not tech.empty:
            work = tech
    rate = sample_rate if sample_rate is not None else enrichment_sample_rate()
    enricher = JobEnricher()
    rows = []
    for _, row in work.iterrows():
        if rate < 1.0 and random.random() > rate:
            continue
        rows.append(enricher.enrich_job(row))
    return pd.DataFrame(rows)

from __future__ import annotations

import json
import random
from typing import Any

import pandas as pd

from ai_gateway.config import enrichment_sample_rate
from ai_gateway.providers.base import validate_json_response
from ai_gateway.router import ModelRouter
from enrichment.schemas import ENRICHMENT_SYSTEM_PROMPT, ENRICHMENT_USER_TEMPLATE


class JobEnricher:
    def __init__(self, router: ModelRouter | None = None):
        self.router = router or ModelRouter()

    def enrich_job(self, row: dict | pd.Series) -> dict[str, Any]:
        title = str(row.get("title", ""))
        company = str(row.get("company", ""))
        location = str(row.get("location", ""))
        description = str(row.get("description", ""))[:2000]
        prompt = ENRICHMENT_USER_TEMPLATE.format(
            title=title, company=company, location=location, description=description
        )
        resp = self.router.complete("enrich", prompt, system=ENRICHMENT_SYSTEM_PROMPT, json_mode=True)
        ok, parsed = validate_json_response(resp.text)
        if not ok or not parsed:
            parsed = {
                "skills": [],
                "seniority": "mid",
                "summary": "",
                "remote_confidence": 0.0,
                "language": "unknown",
            }
        return {
            "job_id": row.get("job_id", ""),
            "ai_skills": json.dumps(parsed.get("skills", [])),
            "ai_seniority": parsed.get("seniority", "mid"),
            "ai_summary": parsed.get("summary", ""),
            "ai_remote_confidence": float(parsed.get("remote_confidence", 0.0)),
            "ai_language": parsed.get("language", "unknown"),
            "ai_model": resp.model_id,
            "ai_provider": resp.provider,
        }


def enrich_jobs_dataframe(df: pd.DataFrame, sample_rate: float | None = None) -> pd.DataFrame:
    """Enrich active jobs; returns ai_job_enrichment DataFrame."""
    if df.empty:
        return pd.DataFrame(
            columns=[
                "job_id", "ai_skills", "ai_seniority", "ai_summary",
                "ai_remote_confidence", "ai_language", "ai_model", "ai_provider",
            ]
        )
    rate = sample_rate if sample_rate is not None else enrichment_sample_rate()
    enricher = JobEnricher()
    rows = []
    for _, row in df.iterrows():
        if rate < 1.0 and random.random() > rate:
            continue
        rows.append(enricher.enrich_job(row))
    return pd.DataFrame(rows)

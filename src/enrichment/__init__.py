"""Batch LLM enrichment for Silver→Gold pipeline."""

from __future__ import annotations

__all__ = ["JobEnricher", "enrich_jobs_dataframe", "classify_job", "classify_field"]


def __getattr__(name: str):
    if name in {"JobEnricher", "enrich_jobs_dataframe"}:
        from enrichment.enricher import JobEnricher, enrich_jobs_dataframe

        return {"JobEnricher": JobEnricher, "enrich_jobs_dataframe": enrich_jobs_dataframe}[name]
    if name in {"classify_job", "classify_field"}:
        from enrichment.rules_de_en import classify_field, classify_job

        return {"classify_job": classify_job, "classify_field": classify_field}[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

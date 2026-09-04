"""Lightweight data quality metrics for Gold layer reporting."""
from datetime import datetime, timezone
from typing import Any, Dict

import pandas as pd

VALID_SOURCES = frozenset({"ba_api", "direct", "eures", "arbeitnow", "berlin_startups"})
REMOVED_SOURCES = frozenset({"indeed", "hacker_news"})


def compute_quality_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    """Compute completeness, uniqueness, freshness, and consistency metrics."""
    now = datetime.now(timezone.utc)
    total = len(df)
    if total == 0:
        return {
            "total_jobs": 0,
            "missing_company_rate": 0.0,
            "missing_title_rate": 0.0,
            "missing_location_rate": 0.0,
            "duplicate_job_id_rate": 0.0,
            "stale_jobs_count": 0,
            "invalid_source_count": 0,
            "schema_validation_pass": False,
            "computed_at": now.isoformat(),
        }

    active = df[df["is_current"] == True] if "is_current" in df.columns else df

    missing_company = active["company"].isna() | (active["company"].astype(str).str.strip() == "")
    missing_title = active["title"].isna() | (active["title"].astype(str).str.strip() == "")
    missing_location = active["location"].isna() | (active["location"].astype(str).str.strip() == "")

    dupes = active["job_id"].duplicated().sum() if "job_id" in active.columns else 0

    stale_count = 0
    if "ingested_at" in active.columns:
        ingested = pd.to_datetime(active["ingested_at"], errors="coerce", utc=True)
        stale_count = int((now - ingested > pd.Timedelta(days=7)).sum())

    invalid_source = 0
    if "source" in active.columns:
        # Removed sources are already outside VALID_SOURCES — one check, no double count.
        invalid_source = int((~active["source"].isin(VALID_SOURCES)).sum())

    remote_in_country_dimension = 0
    if "region" in active.columns:
        remote_in_country_dimension = int((active["region"].astype(str).str.strip() == "Remote").sum())

    required_cols = {"job_id", "title", "company", "location", "source", "url"}
    schema_pass = required_cols.issubset(set(active.columns))

    return {
        "total_jobs": int(len(active)),
        "missing_company_rate": round(float(missing_company.mean()), 4),
        "missing_title_rate": round(float(missing_title.mean()), 4),
        "missing_location_rate": round(float(missing_location.mean()), 4),
        "duplicate_job_id_rate": round(float(dupes / len(active)), 4) if len(active) else 0.0,
        "stale_jobs_count": stale_count,
        "invalid_source_count": invalid_source,
        "remote_in_country_dimension": remote_in_country_dimension,
        "schema_validation_pass": schema_pass and invalid_source == 0 and remote_in_country_dimension == 0,
        "computed_at": now.isoformat(),
    }


def validate_region_taxonomy(regions) -> None:
    """Raise ValueError if work-style 'Remote' appears in the country/region dimension."""
    values = set(str(r).strip() for r in regions)
    if "Remote" in values:
        raise ValueError("Invalid country classification: Remote found in country dimension")


# Internship / CI contracts on Gold quality_report rows (not ingest-time Pydantic).
MAX_MISSING_TITLE_RATE = 0.01
MAX_MISSING_COMPANY_RATE = 0.01
MAX_DUPLICATE_JOB_ID_RATE = 0.0


def evaluate_gold_quality_row(row: Dict[str, Any]) -> list[str]:
    """Return human-readable failures; empty list means the quality gate passed."""
    failures: list[str] = []
    passed = str(row.get("schema_validation_pass", "")).strip().lower() in {"true", "1"}
    if not passed:
        failures.append("schema_validation_pass is not true")
    if float(row.get("missing_title_rate", 1)) > MAX_MISSING_TITLE_RATE:
        failures.append(f"missing_title_rate {row.get('missing_title_rate')} > {MAX_MISSING_TITLE_RATE}")
    if float(row.get("missing_company_rate", 1)) > MAX_MISSING_COMPANY_RATE:
        failures.append(f"missing_company_rate {row.get('missing_company_rate')} > {MAX_MISSING_COMPANY_RATE}")
    if float(row.get("duplicate_job_id_rate", 1)) > MAX_DUPLICATE_JOB_ID_RATE:
        failures.append(f"duplicate_job_id_rate {row.get('duplicate_job_id_rate')} > {MAX_DUPLICATE_JOB_ID_RATE}")
    if int(row.get("invalid_source_count", 1)) != 0:
        failures.append(f"invalid_source_count={row.get('invalid_source_count')}")
    if int(row.get("remote_in_country_dimension", 1)) != 0:
        failures.append("region taxonomy: Remote found in country dimension")
    return failures


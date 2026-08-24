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

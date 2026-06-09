"""Regenerate committed data/gold CSVs from all_jobs using updated Gold logic."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from gold_generator import detect_is_english, detect_language_requirement, detect_work_style  # noqa: E402
from processing.company_normalize import normalize_company  # noqa: E402
from processing.data_quality import compute_quality_metrics, REMOVED_SOURCES  # noqa: E402
from processing.europe_filter import classify_region  # noqa: E402

GOLD = ROOT / "data" / "gold"
REMOVED = REMOVED_SOURCES


def _load_jobs(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, low_memory=False)


def main():
    all_path = GOLD / "all_jobs.csv"
    exp_path = GOLD / "expired_jobs.csv"

    active = _load_jobs(all_path)
    expired = _load_jobs(exp_path)

    for df in (active, expired):
        if df.empty:
            continue
        if "source" in df.columns:
            df.drop(df[df["source"].isin(REMOVED)].index, inplace=True)

    if active.empty:
        print("No active jobs after source filter.")
        return 1

    active = active.rename(columns={"job_url": "url", "is_remote": "remote"}, errors="ignore")
    active["company"] = active["company"].apply(lambda c: normalize_company(c) or str(c or "").strip())

    active["is_english"] = active.apply(detect_is_english, axis=1)
    active["language_requirement"] = active.apply(detect_language_requirement, axis=1)
    active["work_style"] = active.apply(detect_work_style, axis=1)
    active["region"] = active.apply(
        lambda r: classify_region(
            location_str=r.get("location", ""),
            title_str=r.get("title", ""),
            description_str=r.get("description", ""),
        ),
        axis=1,
    )

    current = active.copy()
    silver_like = current.copy()
    silver_like["is_current"] = True

    jobs_by_source = (
        current.groupby("source").size().reset_index(name="job_count").sort_values("job_count", ascending=False)
    )
    jobs_by_region = (
        current.groupby("region").size().reset_index(name="job_count").sort_values("job_count", ascending=False)
    )

    current["location_clean"] = current["location"].astype(str).str.split(",").str[0].str.strip()
    top_locations = (
        current[current["location_clean"].notna() & (current["location_clean"] != "")]
        .groupby("location_clean")
        .size()
        .reset_index(name="job_count")
        .sort_values("job_count", ascending=False)
        .head(20)
        .rename(columns={"location_clean": "location"})
    )

    work_style_labels = {"remote": "Remote", "hybrid": "Hybrid", "onsite": "On-site"}
    remote_vs_onsite = (
        current.groupby("work_style")
        .size()
        .reset_index(name="job_count")
        .rename(columns={"work_style": "work_type"})
    )
    remote_vs_onsite["work_type"] = remote_vs_onsite["work_type"].map(
        lambda x: work_style_labels.get(str(x).lower(), str(x).title())
    )

    companies_df = current.copy()
    companies_df["company"] = companies_df["company"].apply(normalize_company)
    top_companies = (
        companies_df[companies_df["company"].notna()]
        .groupby("company")
        .size()
        .reset_index(name="job_count")
        .sort_values("job_count", ascending=False)
        .head(20)
    )

    english_jobs_total = int(current["is_english"].sum())
    description_insights = pd.DataFrame(
        [{"english_jobs": english_jobs_total, "arbeitnow_english_descriptions": 0, "arbeitnow_total": 0}]
    )
    data_quality_report = pd.DataFrame([compute_quality_metrics(silver_like)])

    # Write outputs
    out_active = current.copy()
    out_active = out_active.rename(columns={"url": "job_url", "remote": "is_remote"}, errors="ignore")
    out_active.to_csv(GOLD / "all_jobs.csv", index=False)
    jobs_by_source.to_csv(GOLD / "jobs_by_source.csv", index=False)
    jobs_by_region.to_csv(GOLD / "jobs_by_region.csv", index=False)
    top_locations.to_csv(GOLD / "top_locations.csv", index=False)
    remote_vs_onsite.to_csv(GOLD / "remote_vs_onsite.csv", index=False)
    top_companies.to_csv(GOLD / "top_companies.csv", index=False)
    description_insights.to_csv(GOLD / "description_insights.csv", index=False)
    data_quality_report.to_csv(GOLD / "data_quality_report.csv", index=False)

    if not expired.empty:
        expired = expired[~expired["source"].isin(REMOVED)].copy()
        expired.to_csv(GOLD / "expired_jobs.csv", index=False)

    print(f"Regenerated Gold CSVs: {len(current)} active jobs, {len(jobs_by_source)} sources")
    print(f"English jobs (title-based): {english_jobs_total}")
    print(remote_vs_onsite.to_string(index=False))
    print("Top companies (first 5):")
    print(top_companies.head().to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""End-to-end validation of Gold layer, metrics API payload, and KPI consistency."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "data" / "gold"
sys.path.insert(0, str(ROOT / "src"))

from processing.company_normalize import normalize_company  # noqa: E402
from gold_generator import detect_is_english  # noqa: E402

VALID_SOURCES = {"ba_api", "direct", "eures", "arbeitnow", "berlin_startups"}
REMOVED = {"indeed", "hacker_news", "apify"}


def _fail(msg: str, errors: list) -> None:
    errors.append(msg)
    print(f"  FAIL: {msg}")


def _ok(msg: str) -> None:
    print(f"  OK: {msg}")


def validate_gold(errors: list) -> dict:
    print("\n=== GOLD LAYER VALIDATION ===")
    all_jobs = pd.read_csv(GOLD / "all_jobs.csv", low_memory=False)
    jobs_by_source = pd.read_csv(GOLD / "jobs_by_source.csv")
    remote = pd.read_csv(GOLD / "remote_vs_onsite.csv")
    top_companies = pd.read_csv(GOLD / "top_companies.csv")
    desc = pd.read_csv(GOLD / "description_insights.csv")

    sources_in_jobs = set(all_jobs["source"].dropna().unique())
    if sources_in_jobs - VALID_SOURCES:
        _fail(f"Invalid sources in all_jobs: {sources_in_jobs - VALID_SOURCES}", errors)
    else:
        _ok(f"Sources in all_jobs: {sorted(sources_in_jobs)}")

    if sources_in_jobs & REMOVED:
        _fail(f"Removed sources still present: {sources_in_jobs & REMOVED}", errors)
    else:
        _ok("No removed sources in all_jobs")

    source_csv = set(jobs_by_source["source"].unique())
    if source_csv != sources_in_jobs:
        _fail(f"jobs_by_source mismatch: csv={source_csv} jobs={sources_in_jobs}", errors)
    else:
        _ok("jobs_by_source matches all_jobs sources")

    if all_jobs["job_id"].duplicated().any():
        _fail(f"Duplicate job_ids: {all_jobs['job_id'].duplicated().sum()}", errors)
    else:
        _ok("No duplicate job_ids")

    for col in ("title", "company", "source"):
        missing = all_jobs[col].isna() | (all_jobs[col].astype(str).str.strip() == "")
        if missing.any():
            _fail(f"Missing {col}: {missing.sum()}", errors)
        else:
            _ok(f"No missing {col}")

    # English KPI
    if "is_english" in all_jobs.columns:
        gold_english = int(
            all_jobs["is_english"].astype(str).str.lower().isin(["true", "1", "yes"]).sum()
        )
    else:
        gold_english = int(all_jobs.apply(detect_is_english, axis=1).sum())
    api_english = int(desc["english_jobs"].iloc[0])
    if gold_english != api_english:
        _fail(f"English KPI mismatch: all_jobs={gold_english} description_insights={api_english}", errors)
    else:
        _ok(f"English KPI consistent: {gold_english}")

    # Remote KPI
    remote_sum = int(remote["job_count"].sum())
    total = len(all_jobs)
    if remote_sum != total:
        _fail(f"Remote breakdown sum {remote_sum} != total jobs {total}", errors)
    else:
        _ok(f"Remote+Hybrid+Onsite sum = total ({total})")

    if (remote["job_count"] < 0).any():
        _fail("Negative remote counts", errors)
    else:
        _ok("No negative remote counts")

    # Top companies placeholders
    bad_companies = []
    for c in top_companies["company"]:
        if normalize_company(c) is None or pd.isna(c) or str(c).strip() == "":
            bad_companies.append(c)
    if bad_companies:
        _fail(f"Placeholder companies in top_companies: {bad_companies[:5]}", errors)
    else:
        _ok("Top companies has no placeholders")

    return {
        "total_jobs": total,
        "english_jobs": gold_english,
        "remote": dict(zip(remote["work_type"], remote["job_count"])),
        "sources": dict(zip(jobs_by_source["source"], jobs_by_source["job_count"])),
    }


def validate_metrics_api_local(stats: dict, errors: list) -> None:
    print("\n=== METRICS API (local Gold mock) ===")
    import os
    from unittest.mock import MagicMock
    import boto3

    os.environ.setdefault("GOLD_BUCKET", "local-mock")

    def mock_get_object(Bucket=None, Key=None, **kwargs):
        body = MagicMock()
        body.read.return_value = (GOLD / Key).read_bytes()
        return {"Body": body}

    boto3.client = lambda service, *a, **k: MagicMock(get_object=mock_get_object) if service == "s3" else MagicMock()

    from metrics_api import _build_payload

    payload = _build_payload("local-mock")

    if payload["total_jobs"] != stats["total_jobs"]:
        _fail(f"API total_jobs {payload['total_jobs']} != Gold {stats['total_jobs']}", errors)
    else:
        _ok(f"API total_jobs = {payload['total_jobs']}")

    if payload["english_jobs"] != stats["english_jobs"]:
        _fail(f"API english_jobs {payload['english_jobs']} != Gold {stats['english_jobs']}", errors)
    else:
        _ok(f"API english_jobs = {payload['english_jobs']}")

    rc = payload["remote_counts"]
    remote_total = rc.get("remote", 0) + rc.get("hybrid", 0) + rc.get("onsite", 0)
    if remote_total != stats["total_jobs"]:
        _fail(f"API remote_counts sum {remote_total} != total {stats['total_jobs']}", errors)
    else:
        _ok(f"API remote_counts sum = total ({remote_total})")

    api_sources = set(payload["jobs_by_source"].keys())
    if api_sources != set(stats["sources"].keys()):
        _fail(f"API sources {api_sources} != Gold sources {set(stats['sources'])}", errors)
    else:
        _ok(f"API sources match Gold ({len(api_sources)} sources)")


def validate_live_api(stats: dict, errors: list) -> None:
    print("\n=== LIVE METRICS API ===")
    import urllib.request

    url = "https://2aww80hwgj.execute-api.eu-central-1.amazonaws.com/"
    try:
        data = json.loads(urllib.request.urlopen(url, timeout=30).read())
    except Exception as e:
        _fail(f"Live API unreachable: {e}", errors)
        return

    if data.get("total_jobs") != stats["total_jobs"]:
        print(f"  WARN: Live API total_jobs={data.get('total_jobs')} vs local Gold={stats['total_jobs']} (S3 may be stale)")
    else:
        _ok(f"Live API total_jobs = {data.get('total_jobs')}")

    live_english = data.get("english_jobs")
    if live_english is None:
        live_english = (data.get("description_insights") or {}).get("english_jobs")
    if live_english is None:
        _fail("Live API missing english_jobs (top-level and description_insights)", errors)
    elif int(live_english) != stats["english_jobs"]:
        print(f"  WARN: Live english {live_english} vs local Gold {stats['english_jobs']} (S3/Gold drift)")
    else:
        _ok(f"Live API english_jobs = {live_english}")

    rc = data.get("remote_counts")
    rv = data.get("remote_vs_onsite") or {}
    if rc:
        _ok(f"Live API remote_counts = {rc}")
    elif rv:
        remote_total = int(rv.get("Remote", 0)) + int(rv.get("Hybrid", 0)) + int(rv.get("On-site", 0))
        if remote_total == stats["total_jobs"]:
            _ok(f"Live API remote_vs_onsite sums to total ({remote_total})")
        else:
            _fail(f"Live remote_vs_onsite sum {remote_total} != total {stats['total_jobs']}", errors)
    else:
        _fail("Live API missing remote KPI fields", errors)


def main():
    errors: list[str] = []
    print("DataForge Pipeline Validation")
    stats = validate_gold(errors)
    validate_metrics_api_local(stats, errors)
    validate_live_api(stats, errors)

    print("\n=== SUMMARY ===")
    if errors:
        print(f"FAILED ({len(errors)} issues):")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

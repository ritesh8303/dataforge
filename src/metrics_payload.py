"""Build and persist dashboard metrics JSON (single S3 GET for the Metrics API)."""

from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import date, datetime, timezone
from io import StringIO

import boto3

s3 = boto3.client("s3")

METRICS_JSON_KEY = "metrics.json"


def _read_csv(bucket: str, key: str) -> list[dict]:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return list(csv.DictReader(StringIO(obj["Body"].read().decode("utf-8-sig"))))


def _count_field(rows: list[dict], *keys: str) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        val = ""
        for key in keys:
            raw = str(row.get(key) or "").strip()
            if raw and raw.lower() not in {"nan", "none", "null"}:
                val = raw
                break
        if not val:
            val = "unspecified"
        counter[val] += 1
    return dict(sorted(counter.items(), key=lambda kv: (-kv[1], kv[0])))


def _coverage_funnel(all_jobs: list[dict], lakehouse_active: int) -> dict:
    by_region = _count_field(all_jobs, "region")
    top_region = dict(list(by_region.items())[:10])
    return {
        "lakehouse_active": int(lakehouse_active or 0),
        "product_jobs": len(all_jobs),
        "by_employment_type": _count_field(all_jobs, "employment_type", "employment_type_rule"),
        "by_field": _count_field(all_jobs, "ai_field", "field", "ai_field_rule", "field_rule"),
        "by_region": top_region,
    }


def _top_companies_early_career(all_jobs: list[dict], top_n: int = 10) -> list[dict]:
    """Product-board early-career density by company (share when lakehouse totals known)."""
    product_counts: Counter[str] = Counter()
    for job in all_jobs:
        company = str(job.get("company") or "").strip()
        if company and company.lower() not in {"nan", "none", "unknown"}:
            product_counts[company] += 1
    ranked = []
    for company, count in product_counts.most_common(top_n):
        ranked.append(
            {
                "company": company,
                "product_jobs": count,
                # Product board is already early-career gated; share unknown without lakehouse company totals.
                "early_career_share": None,
                "company_early_career_score": count,
            }
        )
    return ranked


def company_early_career_map(
    product_jobs: list[dict],
    lakehouse_by_company: dict[str, int] | None = None,
) -> dict[str, dict]:
    """Map company -> early_career_share (or product count when lakehouse totals unavailable)."""
    product_counts: Counter[str] = Counter()
    for job in product_jobs:
        company = str(job.get("company") or "").strip()
        if company:
            product_counts[company] += 1
    out: dict[str, dict] = {}
    for company, prod_n in product_counts.items():
        all_active = None
        if lakehouse_by_company and company in lakehouse_by_company:
            try:
                all_active = int(lakehouse_by_company[company])
            except (TypeError, ValueError):
                all_active = None
        if all_active and all_active > 0:
            share = round(prod_n / all_active, 4)
            out[company] = {
                "product_jobs": prod_n,
                "all_active": all_active,
                "early_career_share": share,
                "company_early_career_score": share,
            }
        else:
            out[company] = {
                "product_jobs": prod_n,
                "all_active": None,
                "early_career_share": None,
                "company_early_career_score": prod_n,
            }
    return out


def build_metrics_payload(bucket: str) -> dict:
    all_jobs = _read_csv(bucket, "all_jobs.csv")
    source_rows = _read_csv(bucket, "jobs_by_source.csv")
    region_rows = _read_csv(bucket, "jobs_by_region.csv")
    location_rows = _read_csv(bucket, "top_locations.csv")
    company_rows = _read_csv(bucket, "top_companies.csv")
    remote_rows = _read_csv(bucket, "remote_vs_onsite.csv")
    status_rows = _read_csv(bucket, "active_vs_expired.csv")
    skill_rows = _read_csv(bucket, "top_skills.csv")
    desc_rows = _read_csv(bucket, "description_insights.csv")
    try:
        stats_rows = _read_csv(bucket, "pipeline_stats.csv")
    except Exception:
        stats_rows = []
    try:
        quality_rows = _read_csv(bucket, "data_quality_report.csv")
    except Exception:
        quality_rows = []

    today = date.today().isoformat()
    jobs_by_source = {r["source"]: int(r["job_count"]) for r in source_rows}
    trend = [
        {"date": r["date"], "count": int(r["new_jobs"])}
        for r in sorted(_read_csv(bucket, "jobs_trend.csv"), key=lambda x: x["date"])[-30:]
    ]
    top_locations = [{"location": r["location"], "count": int(r["job_count"])} for r in location_rows[:10]]
    top_companies = [{"company": r["company"], "count": int(r["job_count"])} for r in company_rows[:10]]
    remote_vs_onsite = {r["work_type"]: int(r["job_count"]) for r in remote_rows}
    remote_counts = {
        "remote": remote_vs_onsite.get("Remote", 0),
        "hybrid": remote_vs_onsite.get("Hybrid", 0),
        "onsite": remote_vs_onsite.get("On-site", 0),
    }
    active_vs_expired = {r["status"]: int(r["job_count"]) for r in status_rows}
    jobs_by_region = {r["region"]: int(r["job_count"]) for r in region_rows}
    top_skills = [{"skill": r["skill"], "count": int(r["job_count"])} for r in skill_rows[:15]]

    desc = desc_rows[0] if desc_rows else {}
    english_jobs = int(desc.get("english_jobs", 0))
    english_jobs_strict = sum(
        1 for j in all_jobs if j.get("language_requirement", "").lower() == "english_only"
    )
    description_insights = {
        "english_jobs": english_jobs,
        "arbeitnow_english_descriptions": int(desc.get("arbeitnow_english_descriptions", 0)),
        "homeoffice_mentioned": int(desc.get("homeoffice_mentioned", 0)),
        "jobs_with_benefits": int(desc.get("jobs_with_benefits", 0)),
        "arbeitnow_total": int(desc.get("arbeitnow_total", 0)),
    }

    stats = stats_rows[0] if stats_rows else {}
    lakehouse_new = int(stats.get("new_jobs", 0) or 0)
    lakehouse_updated = int(stats.get("updated_jobs", 0) or 0)
    lakehouse_expired = int(stats.get("expired_jobs", 0) or 0)
    lakehouse_active = int(stats.get("total_silver", 0) or 0)
    run_at = stats.get("run_at", "") or ""
    run_date = run_at[:10] if len(run_at) >= 10 else today

    # Product KPIs: all_jobs.csv is audience-gated (EU × data/AI × early career)
    product_total = len(all_jobs)
    product_new = sum(1 for j in all_jobs if (j.get("date_added") or "") == run_date)
    if product_new == 0 and run_date != today:
        product_new = sum(1 for j in all_jobs if (j.get("date_added") or "") == today)

    pipeline_stats = {
        "new_jobs": product_new,
        "product_new_jobs": product_new,
        "updated_jobs": lakehouse_updated,
        "expired_jobs": lakehouse_expired,
        "run_at": run_at,
        "lakehouse_new_jobs": lakehouse_new,
        "lakehouse_active": lakehouse_active,
    }

    quality = quality_rows[0] if quality_rows else {}
    data_quality = {
        "missing_company_rate": float(quality.get("missing_company_rate", 0)),
        "missing_location_rate": float(quality.get("missing_location_rate", 0)),
        "duplicate_job_id_rate": float(quality.get("duplicate_job_id_rate", 0)),
        "stale_jobs_count": int(quality.get("stale_jobs_count", 0)),
        "schema_validation_pass": str(quality.get("schema_validation_pass", "false")).lower() == "true",
    }

    lakehouse_total = lakehouse_active or int(active_vs_expired.get("Active", 0))
    coverage_funnel = _coverage_funnel(all_jobs, lakehouse_total)
    top_companies_early_career = _top_companies_early_career(all_jobs, top_n=10)

    if run_at:
        last_updated = run_at.replace("+00:00", "Z") if run_at.endswith("+00:00") else run_at
    else:
        last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return {
        "total_jobs": product_total,
        "new_today": product_new,
        "early_career_jobs": product_total,
        "lakehouse_total_jobs": lakehouse_total,
        "audience": "eu_data_ai_early_career",
        "english_jobs": english_jobs,
        "english_jobs_title_based": english_jobs,
        "english_jobs_strict": english_jobs_strict,
        "remote_counts": remote_counts,
        "jobs_by_source": jobs_by_source,
        "jobs_by_region": jobs_by_region,
        "trend": trend,
        "top_locations": top_locations,
        "top_companies": top_companies,
        "top_companies_early_career": top_companies_early_career,
        "coverage_funnel": coverage_funnel,
        "remote_vs_onsite": remote_vs_onsite,
        "active_vs_expired": active_vs_expired,
        "top_skills": top_skills,
        "description_insights": description_insights,
        "pipeline_stats": pipeline_stats,
        "data_quality": data_quality,
        "last_updated": last_updated,
    }


def upload_metrics_json(bucket: str, payload: dict | None = None) -> dict:
    body = payload if payload is not None else build_metrics_payload(bucket)
    s3.put_object(
        Bucket=bucket,
        Key=METRICS_JSON_KEY,
        Body=json.dumps(body).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=600",
    )
    return body


def load_metrics_json(bucket: str) -> dict | None:
    try:
        obj = s3.get_object(Bucket=bucket, Key=METRICS_JSON_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return None
    except Exception:
        return None

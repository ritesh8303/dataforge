"""Rebuild metrics.json locally from data/gold CSVs using product-first payload logic."""
from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from metrics_payload import (  # noqa: E402
    _board_breakdown,
    _coverage_funnel,
    _product_jobs,
    _top_companies_early_career,
)

GOLD = ROOT / "data" / "gold"


def _read(name: str) -> list[dict]:
    path = GOLD / name
    if not path.exists():
        return []
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def main() -> int:
    all_jobs = _read("all_jobs.csv")
    if not all_jobs:
        print("missing all_jobs.csv")
        return 1
    product = _product_jobs(all_jobs)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    product_board = _board_breakdown(product, run_date=today) if product else {}
    lakehouse_board = _board_breakdown(all_jobs, run_date=today)

    stats_rows = _read("pipeline_stats.csv")
    stats = stats_rows[0] if stats_rows else {}
    quality_rows = _read("data_quality_report.csv")
    quality = quality_rows[0] if quality_rows else {}
    status_rows = _read("active_vs_expired.csv")
    expired = 0
    for r in status_rows:
        if r.get("status") == "Expired":
            expired = int(r.get("job_count") or 0)

    run_at = stats.get("run_at") or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    product_total = len(product)
    board_total = len(all_jobs)
    lakehouse_total = int(stats.get("total_silver") or board_total)
    product_new = sum(1 for j in product if (j.get("date_added") or "") == today)
    english_strict = int(product_board.get("english_jobs_strict") or 0)

    payload = {
        "total_jobs": product_total,
        "board_jobs": board_total,
        "new_today": product_new or int(product_board.get("board_new_jobs") or 0),
        "early_career_jobs": product_total,
        "product_jobs": product_total,
        "lakehouse_total_jobs": lakehouse_total,
        "audience": "eu_data_ai_early_career",
        "audience_product": "eu_data_ai_early_career",
        "headline_scope": "product_early_career",
        "scope_note": (
            "Headline KPIs use the EU data/AI early-career product gate. "
            "Lakehouse totals (all active tech postings) are secondary. "
            "English Only badges use language_requirement=english_only (strict). "
            "Always verify openings on the employer site before applying."
        ),
        "english_jobs": english_strict,
        "english_jobs_title_based": int(product_board.get("english_jobs") or 0),
        "english_jobs_strict": english_strict,
        "remote_counts": product_board.get("remote_counts") or {},
        "jobs_by_source": product_board.get("jobs_by_source") or {},
        "jobs_by_region": product_board.get("jobs_by_region") or {},
        "trend": [
            {
                "date": r["date"],
                "count": int(r["new_jobs"]),
                "scope": "product_early_career",
            }
            for r in sorted(_read("jobs_trend.csv"), key=lambda x: x["date"])[-30:]
        ],
        "trend_scope": "product_early_career",
        "top_locations": product_board.get("top_locations") or [],
        "top_companies": product_board.get("top_companies") or [],
        "top_companies_early_career": _top_companies_early_career(product, top_n=10),
        "coverage_funnel": {
            **_coverage_funnel(product, lakehouse_total),
            "published_active": board_total,
        },
        "remote_vs_onsite": product_board.get("remote_vs_onsite") or {},
        "active_vs_expired": {"Active": board_total, "Expired": expired},
        "top_skills": product_board.get("top_skills") or [],
        "trust_tiers": product_board.get("trust_tiers") or {},
        "description_insights": (_read("description_insights.csv") or [{}])[0],
        "pipeline_stats": {
            "new_jobs": product_new,
            "board_new_jobs": int(lakehouse_board.get("board_new_jobs") or 0),
            "product_new_jobs": product_new,
            "updated_jobs": int(stats.get("updated_jobs") or 0),
            "expired_jobs": int(stats.get("expired_jobs") or 0),
            "run_at": run_at,
            "lakehouse_new_jobs": int(stats.get("new_jobs") or 0),
            "lakehouse_active": lakehouse_total,
            "board_jobs": board_total,
        },
        "data_quality": {
            "missing_company_rate": float(quality.get("missing_company_rate") or 0),
            "missing_location_rate": float(quality.get("missing_location_rate") or 0),
            "duplicate_job_id_rate": float(quality.get("duplicate_job_id_rate") or 0),
            "stale_jobs_count": int(
                product_board.get("stale_jobs_count")
                or quality.get("stale_jobs_count")
                or 0
            ),
            "schema_validation_pass": str(quality.get("schema_validation_pass", "true")).lower()
            == "true",
        },
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    for dest in (GOLD / "metrics.json", ROOT / "docs" / "metrics.json"):
        dest.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        print(f"wrote {dest} product={product_total} lakehouse={board_total} english_strict={english_strict}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

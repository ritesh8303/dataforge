"""Unit economics model for thesis ROI chapter."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Assumptions — update with real router cost logs from evals/results/
ASSUMPTIONS = {
    "cost_per_match_usd": 0.0002,
    "cost_per_enriched_job_usd": 0.0005,
    "price_freemium_match_usd": 0.0,
    "price_premium_match_usd": 0.02,
    "price_b2b_report_monthly_usd": 199.0,
    "price_enrichment_per_1k_jobs_usd": 5.0,
    "fixed_infra_monthly_usd": 3.0,
}


def break_even_mau(price_per_match: float, cost_per_match: float, fixed: float) -> float:
    margin = price_per_match - cost_per_match
    if margin <= 0:
        return float("inf")
    return fixed / margin


def main():
    a = ASSUMPTIONS
    scenarios = {
        "freemium_match_api": {
            "revenue_per_1k_matches": a["price_freemium_match_usd"] * 1000,
            "cost_per_1k_matches": a["cost_per_match_usd"] * 1000,
            "break_even_mau": break_even_mau(a["price_freemium_match_usd"], a["cost_per_match_usd"], a["fixed_infra_monthly_usd"]),
        },
        "premium_match_api": {
            "revenue_per_1k_matches": a["price_premium_match_usd"] * 1000,
            "cost_per_1k_matches": a["cost_per_match_usd"] * 1000,
            "margin_per_1k": (a["price_premium_match_usd"] - a["cost_per_match_usd"]) * 1000,
        },
        "b2b_labour_reports": {
            "monthly_price": a["price_b2b_report_monthly_usd"],
            "customers_to_cover_infra": max(1, int(a["fixed_infra_monthly_usd"] / a["price_b2b_report_monthly_usd"]) + 1),
        },
        "usage_based_enrichment": {
            "revenue_per_1k_jobs": a["price_enrichment_per_1k_jobs_usd"],
            "cost_per_1k_jobs": a["cost_per_enriched_job_usd"] * 1000,
            "margin_per_1k": a["price_enrichment_per_1k_jobs_usd"] - a["cost_per_enriched_job_usd"] * 1000,
        },
    }

    out_dir = ROOT / "evals" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "roi_model.json"
    payload = {"assumptions": a, "scenarios": scenarios}
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()

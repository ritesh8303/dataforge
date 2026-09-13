#!/usr/bin/env python3
"""Unit-economics report from CostLogger records or pricing model.

Usage:
  py -3 scripts/roi_report.py
  py -3 scripts/roi_report.py --records path/to/cost_log.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ai_gateway.config import MODEL_PRICING  # noqa: E402
from ai_gateway.cost_logger import CostLogger  # noqa: E402

EUR_PER_USD = 0.92  # thesis display FX; not a live rate feed


def _from_records(path: Path) -> dict:
    from ai_gateway.cost_logger import UsageRecord

    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("records") or []
    logger = CostLogger()
    fields = UsageRecord.__dataclass_fields__
    for r in records:
        logger.records.append(UsageRecord(**{k: r[k] for k in fields if k in r}))
    return logger.summary()


def _model_scenario() -> dict:
    """Estimate € for 1k enrichments + 100 hybrid matches (Nova Micro + Titan Embed)."""
    enrich_model = "amazon.nova-micro-v1:0"
    embed_model = "amazon.titan-embed-text-v2:0"
    pricing_e = MODEL_PRICING.get(enrich_model)
    pricing_v = MODEL_PRICING.get(embed_model)
    # Assumed tokens per job (thesis-order-of-magnitude)
    enrich_in, enrich_out = 1200, 250
    embed_in = 400
    n_enrich, n_match = 1000, 100
    cost_enrich = 0.0
    cost_embed = 0.0
    if pricing_e:
        cost_enrich = n_enrich * (
            (enrich_in / 1000.0) * pricing_e.input_per_1k + (enrich_out / 1000.0) * pricing_e.output_per_1k
        )
    if pricing_v:
        cost_embed = (n_enrich + n_match) * ((embed_in / 1000.0) * pricing_v.input_per_1k)
    # Rule-first: assume 70% skip LLM enrich
    cost_enrich_rules_first = cost_enrich * 0.3
    total_usd = cost_enrich_rules_first + cost_embed
    return {
        "scenario": "1000_jobs_rules_first_plus_100_matches",
        "assumptions": {
            "enrich_sample_rate_llm": 0.3,
            "tokens_enrich_in_out": [enrich_in, enrich_out],
            "tokens_embed_in": embed_in,
            "fx_eur_per_usd": EUR_PER_USD,
        },
        "cost_usd": {
            "enrich_llm_only_full": round(cost_enrich, 4),
            "enrich_rules_first": round(cost_enrich_rules_first, 4),
            "embeddings": round(cost_embed, 4),
            "total": round(total_usd, 4),
            "per_enriched_job": round(cost_enrich_rules_first / n_enrich, 6),
            "per_match_request_embed_share": round(cost_embed / (n_enrich + n_match), 6),
        },
        "cost_eur_approx": {
            "total": round(total_usd * EUR_PER_USD, 4),
            "per_enriched_job": round((cost_enrich_rules_first / n_enrich) * EUR_PER_USD, 6),
        },
        "models": {"enrich": enrich_model, "embed": embed_model},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=Path, default=None)
    args = parser.parse_args()

    report = {
        "model_scenario": _model_scenario(),
        "live_summary": None,
        "note": "Prefer live CostLogger JSON when available; model_scenario is thesis-order-of-magnitude.",
    }
    if args.records and args.records.exists():
        report["live_summary"] = _from_records(args.records)

    out = ROOT / "evals" / "results" / "roi_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

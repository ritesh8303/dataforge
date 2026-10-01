#!/usr/bin/env python3
"""Unit-economics report from CostLogger records or pricing model.

Usage:
  py -3 scripts/roi_report.py
  py -3 scripts/roi_report.py --records data/ai_cost_records.json

Reads CostLogger flush JSON (records + summary) when present, writes:
  - evals/results/roi_report.json
  - docs/roi.html  (€/1k jobs + by_provider)
"""

from __future__ import annotations

import argparse
import html
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ai_gateway.config import MODEL_PRICING  # noqa: E402
from ai_gateway.cost_logger import CostLogger  # noqa: E402

EUR_PER_USD = 0.92  # thesis display FX; not a live rate feed
DEFAULT_RECORDS = ROOT / "data" / "ai_cost_records.json"


def _from_records(path: Path) -> dict:
    from ai_gateway.cost_logger import UsageRecord

    payload = json.loads(path.read_text(encoding="utf-8"))
    # Accept either a full flush {"records":[...],"summary":{...}} or summary-only.
    if isinstance(payload.get("summary"), dict) and not payload.get("records"):
        return dict(payload["summary"])

    records = payload.get("records") or []
    if not records and all(k in payload for k in ("total_cost_usd", "total_calls")):
        return dict(payload)

    logger = CostLogger()
    fields = UsageRecord.__dataclass_fields__
    for r in records:
        logger.records.append(UsageRecord(**{k: r[k] for k in fields if k in r}))
    return logger.summary()


def _model_scenario() -> dict:
    """Estimate € for 1k enrichments + 100 hybrid matches (OpenAI-first production path)."""
    enrich_model = "gpt-4o-mini"
    embed_model = "text-embedding-3-small"
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
    # Rule-first: assume 70% skip LLM enrich (AI_ENRICHMENT_SAMPLE_RATE≈0.25–0.3)
    cost_enrich_rules_first = cost_enrich * 0.3
    total_usd = cost_enrich_rules_first + cost_embed
    return {
        "scenario": "1000_jobs_rules_first_plus_100_matches_openai",
        "assumptions": {
            "enrich_sample_rate_llm": 0.3,
            "tokens_enrich_in_out": [enrich_in, enrich_out],
            "tokens_embed_in": embed_in,
            "fx_eur_per_usd": EUR_PER_USD,
            "n_jobs": n_enrich,
            "provider": "openai",
        },
        "cost_usd": {
            "enrich_llm_only_full": round(cost_enrich, 4),
            "enrich_rules_first": round(cost_enrich_rules_first, 4),
            "embeddings": round(cost_embed, 4),
            "total": round(total_usd, 4),
            "per_enriched_job": round(cost_enrich_rules_first / n_enrich, 6),
            "per_match_request_embed_share": round(cost_embed / (n_enrich + n_match), 6),
            "per_1k_jobs": round(total_usd, 4),
        },
        "cost_eur_approx": {
            "total": round(total_usd * EUR_PER_USD, 4),
            "per_enriched_job": round((cost_enrich_rules_first / n_enrich) * EUR_PER_USD, 6),
            "per_1k_jobs": round(total_usd * EUR_PER_USD, 4),
        },
        "models": {"enrich": enrich_model, "embed": embed_model},
        "bedrock_fallback_note": "Nova Micro + Titan remain cheaper when Bedrock quota is approved.",
    }


def _live_unit_economics(summary: dict) -> dict:
    """Derive €/1k jobs from a CostLogger summary when call counts are available."""
    total_usd = float(summary.get("total_cost_usd") or 0.0)
    by_task = summary.get("by_task") or {}
    enrich_calls = int((by_task.get("enrich") or {}).get("calls") or 0)
    # Prefer enrich call count as proxy for jobs processed; else total_calls.
    jobs = enrich_calls or int(summary.get("total_calls") or 0)
    per_job_usd = (total_usd / jobs) if jobs else 0.0
    per_1k_usd = per_job_usd * 1000.0
    by_provider = {}
    for name, bucket in (summary.get("by_provider") or {}).items():
        cost = float(bucket.get("cost_usd") or 0.0)
        by_provider[name] = {
            "calls": int(bucket.get("calls") or 0),
            "cost_usd": round(cost, 6),
            "cost_eur": round(cost * EUR_PER_USD, 6),
            "failures": int(bucket.get("failures") or 0),
        }
    return {
        "jobs_proxy": jobs,
        "total_cost_usd": round(total_usd, 6),
        "total_cost_eur": round(total_usd * EUR_PER_USD, 6),
        "eur_per_1k_jobs": round(per_1k_usd * EUR_PER_USD, 6),
        "usd_per_1k_jobs": round(per_1k_usd, 6),
        "by_provider": by_provider,
        "fx_eur_per_usd": EUR_PER_USD,
    }


def _render_html(report: dict) -> str:
    scenario = report.get("model_scenario") or {}
    eur_1k = (scenario.get("cost_eur_approx") or {}).get("per_1k_jobs")
    live = report.get("live_unit_economics")
    live_rows = ""
    if live and live.get("by_provider"):
        cells = []
        for name, b in live["by_provider"].items():
            cells.append(
                "<tr>"
                f"<td>{html.escape(str(name))}</td>"
                f"<td>{b.get('calls', 0)}</td>"
                f"<td>{b.get('cost_usd', 0):.6f}</td>"
                f"<td>{b.get('cost_eur', 0):.6f}</td>"
                "</tr>"
            )
        live_rows = "\n".join(cells)
    live_block = ""
    if live:
        live_block = f"""
  <h2>Live CostLogger</h2>
  <p>€/1k jobs (proxy): <strong>{live.get('eur_per_1k_jobs', 0):.4f}</strong>
     · total € {live.get('total_cost_eur', 0):.4f}
     · jobs proxy {live.get('jobs_proxy', 0)}</p>
  <table>
    <thead><tr><th>Provider</th><th>Calls</th><th>USD</th><th>EUR</th></tr></thead>
    <tbody>
{live_rows or '<tr><td colspan="4">No by_provider data</td></tr>'}
    </tbody>
  </table>
"""
    else:
        live_block = "<h2>Live CostLogger</h2><p>No <code>data/ai_cost_records.json</code> yet — modelled scenario only.</p>"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>DataForge — ROI / unit economics</title>
  <link rel="icon" href="df-logo.png" type="image/png" />
  <link rel="stylesheet" href="components/design-system.css" />
  <style>
    .roi-wrap {{ max-width: 720px; margin: 0 auto; padding: 2.5rem 1rem 4rem; }}
    .roi-wrap h1 {{ font-family: var(--df-font-serif, Georgia, serif); font-weight: 400; font-size: 2rem; }}
    .roi-wrap h2 {{ font-size: 1.1rem; margin-top: 2rem; }}
    .roi-wrap p, .roi-wrap td, .roi-wrap th {{ color: var(--df-text-secondary, #445); line-height: 1.6; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 0.75rem; }}
    th, td {{ text-align: left; padding: 0.4rem 0.5rem; border-bottom: 1px solid #dde3ee; font-size: 0.9rem; }}
    .metric {{ font-size: 1.75rem; color: var(--df-text, #122); }}
  </style>
</head>
<body>
<nav class="df-navbar">
  <div class="df-navbar-inner">
    <a href="index.html" class="df-navbar-brand">
      <img src="df-logo.png" alt="DataForge" />
      DataForge
    </a>
    <div class="df-navbar-links">
      <a href="index.html">Home</a>
      <a href="dashboard.html">Dashboard</a>
      <a href="roi.html" class="active">ROI</a>
      <a href="about.html">About</a>
    </div>
  </div>
</nav>
<main class="roi-wrap">
  <p style="letter-spacing:0.1em;text-transform:uppercase;font-size:0.75rem;color:#889;">Thesis · RQ4 unit economics</p>
  <h1>ROI report</h1>
  <p>Modelled cost for 1k jobs (rules-first enrich + embeddings):</p>
  <p class="metric">€{eur_1k if eur_1k is not None else "—"} <span style="font-size:1rem;">/ 1k jobs</span></p>
  <p>USD total {((scenario.get("cost_usd") or {}).get("total"))} · FX {EUR_PER_USD} EUR/USD</p>
{live_block}
  <p style="margin-top:2rem;font-size:0.85rem;">Generated by <code>scripts/roi_report.py</code>. Source JSON: <code>evals/results/roi_report.json</code>.</p>
</main>
</body>
</html>
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--records",
        type=Path,
        default=DEFAULT_RECORDS,
        help=f"CostLogger flush JSON (default: {DEFAULT_RECORDS})",
    )
    args = parser.parse_args()

    report = {
        "model_scenario": _model_scenario(),
        "live_summary": None,
        "live_unit_economics": None,
        "records_path": str(args.records),
        "note": "Prefer live CostLogger JSON when available; model_scenario is thesis-order-of-magnitude.",
    }
    if args.records.exists():
        summary = _from_records(args.records)
        report["live_summary"] = summary
        report["live_unit_economics"] = _live_unit_economics(summary)

    out = ROOT / "evals" / "results" / "roi_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    html_path = ROOT / "docs" / "roi.html"
    html_path.write_text(_render_html(report), encoding="utf-8")

    print(json.dumps(report, indent=2))
    print(f"Wrote {out}")
    print(f"Wrote {html_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

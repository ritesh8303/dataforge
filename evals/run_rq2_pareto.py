#!/usr/bin/env python3
"""RQ2 Pareto: rules vs live LLM providers (Bedrock-first) on a capped sample.

Examples:
  py -3 evals/run_rq2_pareto.py
  py -3 evals/run_rq2_pareto.py --live-providers --provider bedrock --limit 40
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ai_gateway.config import TASK_PROFILES  # noqa: E402
from ai_gateway.router import ModelRouter  # noqa: E402
from enrichment.rules_de_en import classify_job  # noqa: E402


def _score_rules(job: dict) -> dict:
    t0 = time.perf_counter()
    out = classify_job(
        title=str(job.get("title") or ""),
        description=str(job.get("description") or ""),
        tags=str(job.get("tags") or ""),
    )
    ms = (time.perf_counter() - t0) * 1000
    return {
        "provider": "rules",
        "latency_ms": round(ms, 2),
        "cost_usd": 0.0,
        "field": out.get("field_rule"),
        "ok": True,
    }


def _score_llm(job: dict, providers: list[str], task: str = "enrich") -> dict:
    # Temporarily pin task profile to requested providers only (no OpenAI/Anthropic spill).
    original = TASK_PROFILES.get(task, {}).get("preferred_providers")
    if task in TASK_PROFILES:
        TASK_PROFILES[task]["preferred_providers"] = providers

    router = ModelRouter()
    prompt = json.dumps(
        {
            "title": job.get("title"),
            "description": str(job.get("description") or "")[:600],
            "ask": 'Return JSON {"ai_field":"...","ai_seniority":"..."}',
        }
    )
    t0 = time.perf_counter()
    try:
        resp = router.complete(task, prompt, system="Classify the job. JSON only.", json_mode=True)
        ms = (time.perf_counter() - t0) * 1000
        # Per-call cost from last successful log entry
        cost = 0.0
        if router.cost_logger.records:
            cost = float(router.cost_logger.records[-1].cost_usd)
        return {
            "provider": getattr(resp, "provider", "unknown"),
            "model_id": getattr(resp, "model_id", ""),
            "latency_ms": round(ms, 2),
            "cost_usd": round(cost, 6),
            "ok": True,
            "text_preview": (resp.text or "")[:160],
        }
    except Exception as exc:
        ms = (time.perf_counter() - t0) * 1000
        return {
            "provider": "error",
            "latency_ms": round(ms, 2),
            "cost_usd": 0.0,
            "ok": False,
            "error": str(exc)[:300],
        }
    finally:
        if original is not None and task in TASK_PROFILES:
            TASK_PROFILES[task]["preferred_providers"] = original


def _agg(rows: list[dict], key: str) -> dict:
    items = [r[key] for r in rows if key in r]
    ok = [i for i in items if i.get("ok")]
    lat = [float(i["latency_ms"]) for i in ok]
    cost = sum(float(i.get("cost_usd") or 0) for i in items)
    providers = {}
    for i in ok:
        p = str(i.get("provider") or "unknown")
        providers[p] = providers.get(p, 0) + 1
    return {
        "n": len(items),
        "n_ok": len(ok),
        "avg_latency_ms": round(sum(lat) / len(lat), 2) if lat else 0,
        "p50_latency_ms": round(float(median(lat)), 2) if lat else 0,
        "p95_latency_ms": round(sorted(lat)[max(0, int(len(lat) * 0.95) - 1)], 2) if lat else 0,
        "total_cost_usd": round(cost, 6),
        "avg_cost_usd": round(cost / len(items), 6) if items else 0,
        "providers": providers,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=40)
    parser.add_argument("--live-providers", action="store_true")
    parser.add_argument(
        "--provider",
        default="bedrock",
        choices=["bedrock", "openai", "anthropic", "local"],
        help="When --live-providers, pin cascade to this provider only",
    )
    args = parser.parse_args()

    jobs = json.loads((ROOT / "evals" / "data" / "sample_jobs.json").read_text(encoding="utf-8"))[: args.limit]
    rows = []
    for job in jobs:
        row = {"job_id": job.get("job_id"), "rules": _score_rules(job)}
        if args.live_providers:
            row["llm"] = _score_llm(job, providers=[args.provider])
        rows.append(row)

    report = {
        "n_jobs": len(jobs),
        "live_providers": args.live_providers,
        "pinned_provider": args.provider if args.live_providers else None,
        "bedrock_region": os.environ.get("AWS_BEDROCK_REGION", "eu-central-1"),
        "completion_model": os.environ.get("BEDROCK_COMPLETION_MODEL", "amazon.titan-text-express-v1"),
        "rules": _agg(rows, "rules"),
        "llm": _agg(rows, "llm") if args.live_providers else None,
        "note": (
            "Rules = €0 baseline. Live point uses pinned provider only (default bedrock). "
            "Costs are CostLogger estimates from MODEL_PRICING."
        ),
        "rows_sample": rows[:5],
    }
    out = ROOT / "evals" / "results" / "rq2_pareto.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    printable = {k: report[k] for k in report if k != "rows_sample"}
    print(json.dumps(printable, indent=2))
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

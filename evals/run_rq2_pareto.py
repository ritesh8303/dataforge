#!/usr/bin/env python3
"""RQ2 Pareto stub: quality vs latency vs € across providers on a small sample.

Default uses the local provider only (CI-safe). Pass --live-providers to attempt
OpenAI/Anthropic/Bedrock when keys exist — still capped to --limit jobs.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

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
    return {"provider": "rules", "latency_ms": round(ms, 2), "cost_usd": 0.0, "field": out.get("field_rule"), "ok": True}


def _score_router(job: dict, task: str = "enrich") -> dict:
    router = ModelRouter()
    prompt = json.dumps(
        {
            "title": job.get("title"),
            "description": str(job.get("description") or "")[:600],
            "ask": "Return JSON {\"ai_field\":\"...\",\"ai_seniority\":\"...\"}",
        }
    )
    t0 = time.perf_counter()
    try:
        resp = router.complete(task, prompt, system="Classify the job. JSON only.", json_mode=True, task=task)
        ms = (time.perf_counter() - t0) * 1000
        summary = router.cost_logger.summary()
        return {
            "provider": resp.provider if hasattr(resp, "provider") else summary.get("by_provider", {}),
            "latency_ms": round(ms, 2),
            "cost_usd": summary.get("total_cost_usd", 0.0),
            "ok": True,
            "text_preview": (resp.text or "")[:120],
        }
    except Exception as exc:
        ms = (time.perf_counter() - t0) * 1000
        return {"provider": "error", "latency_ms": round(ms, 2), "cost_usd": 0.0, "ok": False, "error": str(exc)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--live-providers", action="store_true")
    args = parser.parse_args()

    jobs = json.loads((ROOT / "evals" / "data" / "sample_jobs.json").read_text(encoding="utf-8"))[: args.limit]
    rows = []
    for job in jobs:
        rows.append({"job_id": job.get("job_id"), "rules": _score_rules(job)})
        if args.live_providers:
            rows.append({"job_id": job.get("job_id"), "llm": _score_router(job)})

    # Aggregate
    rules_lat = [r["rules"]["latency_ms"] for r in rows if "rules" in r]
    report = {
        "n_jobs": len(jobs),
        "live_providers": args.live_providers,
        "rules": {
            "avg_latency_ms": round(sum(rules_lat) / len(rules_lat), 2) if rules_lat else 0,
            "cost_usd": 0.0,
            "p50_latency_ms": sorted(rules_lat)[len(rules_lat) // 2] if rules_lat else 0,
        },
        "note": (
            "CI default = rules-only Pareto point. "
            "Run with --live-providers under budget for Bedrock/OpenAI/Anthropic points (thesis RQ2)."
        ),
        "rows_sample": rows[:5],
    }
    out = ROOT / "evals" / "results" / "rq2_pareto.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({k: report[k] for k in report if k != "rows_sample"}, indent=2))
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

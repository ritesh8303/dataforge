#!/usr/bin/env python3
"""RQ2 Pareto: rules vs live LLM providers (Bedrock-first) on a capped sample.

Examples:
  py -3 evals/run_rq2_pareto.py
  py -3 evals/run_rq2_pareto.py --live-providers --provider bedrock --limit 40
  # When Bedrock quota is 0, use OpenAI (~€0.004 for 40 jobs with gpt-4o-mini):
  set OPENAI_API_KEY=...
  set AI_ENABLED=true
  py -3 evals/run_rq2_pareto.py --live-providers --provider openai --limit 40
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


def _score_llm(job: dict, providers: list[str], task: str = "enrich", *, strict: bool = True) -> dict:
    """Score one job via LLM. When strict=True (default), call the pinned provider only —
    do not count ModelRouter's local fallback as a successful live cell.
    """
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
        pinned = providers[0] if providers else "local"
        if strict and pinned != "local":
            provider = router._providers.get(pinned)
            if provider is None or not provider.available():
                raise RuntimeError(f"Pinned provider '{pinned}' is not available (missing API key?)")
            resp = provider.complete(
                prompt,
                system="Classify the job. JSON only.",
                json_mode=True,
            )
            from ai_gateway.providers.base import validate_json_response

            ok_json, _ = validate_json_response(resp.text or "")
            if not ok_json:
                raise ValueError(f"Invalid JSON from {pinned}")
            router.cost_logger.log(
                task=task,
                provider=resp.provider,
                model_id=resp.model_id,
                input_tokens=resp.input_tokens,
                output_tokens=resp.output_tokens,
                latency_ms=resp.latency_ms,
                success=True,
            )
        else:
            resp = router.complete(task, prompt, system="Classify the job. JSON only.", json_mode=True)

        ms = (time.perf_counter() - t0) * 1000
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
            "provider": providers[0] if providers else "error",
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
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Seconds to sleep between live LLM calls (rate-limit friendly)",
    )
    parser.add_argument(
        "--allow-local-fallback",
        action="store_true",
        help="Allow ModelRouter local fallback (default: strict pinned provider only)",
    )
    args = parser.parse_args()

    jobs = json.loads((ROOT / "evals" / "data" / "sample_jobs.json").read_text(encoding="utf-8"))[: args.limit]
    rows = []
    for i, job in enumerate(jobs):
        row = {"job_id": job.get("job_id"), "rules": _score_rules(job)}
        if args.live_providers:
            if i and args.sleep > 0:
                time.sleep(args.sleep)
            row["llm"] = _score_llm(
                job,
                providers=[args.provider],
                strict=not args.allow_local_fallback,
            )
            status = "ok" if row["llm"].get("ok") else f"FAIL:{row['llm'].get('error', '')[:80]}"
            print(f"[{i + 1}/{len(jobs)}] {job.get('job_id')} {status}", flush=True)
        rows.append(row)

    completion_model = {
        "openai": os.environ.get("OPENAI_COMPLETION_MODEL", "gpt-4o-mini"),
        "anthropic": os.environ.get("ANTHROPIC_COMPLETION_MODEL", "claude-3-haiku-20240307"),
        "bedrock": os.environ.get("BEDROCK_COMPLETION_MODEL", "eu.amazon.nova-micro-v1:0"),
        "local": "local-heuristic",
    }.get(args.provider if args.live_providers else "", os.environ.get("BEDROCK_COMPLETION_MODEL", ""))

    report = {
        "n_jobs": len(jobs),
        "live_providers": args.live_providers,
        "pinned_provider": args.provider if args.live_providers else None,
        "strict_pin": bool(args.live_providers and not args.allow_local_fallback),
        "bedrock_region": os.environ.get("AWS_BEDROCK_REGION", "eu-central-1"),
        "completion_model": completion_model,
        "rules": _agg(rows, "rules"),
        "llm": _agg(rows, "llm") if args.live_providers else None,
        "note": (
            "Rules = €0 baseline. Live point uses pinned provider only (default bedrock). "
            "Strict pin rejects silent local fallback. Costs from CostLogger / MODEL_PRICING."
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

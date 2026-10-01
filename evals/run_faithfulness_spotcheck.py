#!/usr/bin/env python3
"""Citation faithfulness spot-check (structure + optional OpenAI judge).

Usage:
  py -3 evals/run_faithfulness_spotcheck.py --limit 10
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def _evidence_in_job(evidence: str, job: dict) -> bool:
    ev = (evidence or "").strip().lower()
    if len(ev) < 8:
        return False
    hay = " ".join(
        str(job.get(k) or "")
        for k in ("title", "description", "tags", "company", "location")
    ).lower()
    # Allow short snippet / token overlap match
    if ev in hay:
        return True
    tokens = [tok for tok in ev.split() if len(tok) > 5][:3]
    return any(tok in hay for tok in tokens)


def _openai_judge(job: dict, citation: dict) -> dict:
    from ai_gateway.router import ModelRouter
    from ai_gateway.providers.base import validate_json_response

    router = ModelRouter()
    system = (
        'Judge if the citation is faithful to the job text. '
        'Return JSON {"faithful":true|false,"score":0-1,"note":"..."} only.'
    )
    prompt = json.dumps(
        {
            "title": job.get("title"),
            "description": str(job.get("description") or "")[:900],
            "citation": citation,
        },
        ensure_ascii=False,
    )
    resp = router.complete("explain", prompt, system=system, json_mode=True)
    ok, parsed = validate_json_response(resp.text)
    if not ok or not isinstance(parsed, dict):
        return {"faithful": False, "score": 0.0, "note": "invalid_judge_json", "provider": resp.provider}
    return {
        "faithful": bool(parsed.get("faithful")),
        "score": float(parsed.get("score") or 0),
        "note": str(parsed.get("note") or "")[:200],
        "provider": resp.provider,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--use-llm-judge", action="store_true")
    args = parser.parse_args()

    from api.match_service import match_jobs

    payload = match_jobs(
        resume="Python SQL Spark Airflow AWS. Seeking working student or junior data engineer.",
        dream_role="Data Engineer",
        location="Berlin",
        method="hybrid",
        limit=args.limit,
        entry_level_only=True,
        data_ai_only=True,
        audience_only=True,
    )
    rows = []
    for job in payload.get("jobs") or []:
        for cite in job.get("citations") or []:
            structural = _evidence_in_job(str(cite.get("evidence") or ""), job) or bool(cite.get("reason"))
            item = {
                "job_id": job.get("job_id"),
                "title": job.get("title"),
                "reason": cite.get("reason"),
                "evidence": cite.get("evidence"),
                "structural_ok": structural,
            }
            if args.use_llm_judge and os.environ.get("OPENAI_API_KEY") and os.environ.get("AI_ENABLED", "true").lower() != "false":
                try:
                    item["llm_judge"] = _openai_judge(job, cite)
                except Exception as exc:  # noqa: BLE001
                    item["llm_judge"] = {"error": str(exc)[:200]}
            rows.append(item)

    structural_rate = (sum(1 for r in rows if r.get("structural_ok")) / len(rows)) if rows else 0.0
    llm_rows = [r for r in rows if isinstance(r.get("llm_judge"), dict) and "faithful" in r["llm_judge"]]
    llm_rate = (sum(1 for r in llm_rows if r["llm_judge"].get("faithful")) / len(llm_rows)) if llm_rows else None

    out = {
        "n_citations": len(rows),
        "structural_faithfulness_rate": round(structural_rate, 4),
        "llm_faithfulness_rate": None if llm_rate is None else round(llm_rate, 4),
        "method": payload.get("method"),
        "cost_summary": payload.get("cost_summary"),
        "rows": rows,
    }
    dest = ROOT / "evals" / "results" / "faithfulness_spotcheck.json"
    dest.write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")
    md = ROOT / "evals" / "results" / "faithfulness_spotcheck.md"
    md.write_text(
        f"# Faithfulness spot-check\n\n"
        f"- Citations: {out['n_citations']}\n"
        f"- Structural rate: {out['structural_faithfulness_rate']}\n"
        f"- LLM judge rate: {out['llm_faithfulness_rate']}\n"
        f"- Retrieval method: `{out['method']}`\n",
        encoding="utf-8",
    )
    print(json.dumps({k: out[k] for k in out if k != "rows"}, indent=2))
    print(f"Wrote {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

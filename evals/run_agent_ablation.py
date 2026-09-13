"""Ablation: single-shot hybrid vs multi-agent citation validity (local provider)."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from api.match_service import match_jobs
from agent.graph import run_match_agent


def _citation_validity(jobs: list[dict]) -> float:
    if not jobs:
        return 0.0
    ok = 0
    total = 0
    for job in jobs:
        jid = job.get("job_id")
        for c in job.get("citations") or []:
            total += 1
            if c.get("job_id") == jid and c.get("reason") and c.get("evidence") is not None:
                ok += 1
        if not (job.get("citations") or []):
            total += 1
    return ok / total if total else 0.0


def main() -> int:
    labels = json.loads((ROOT / "evals" / "data" / "label_queries.json").read_text(encoding="utf-8"))[:30]
    jobs = json.loads((ROOT / "evals" / "data" / "sample_jobs.json").read_text(encoding="utf-8"))
    # Build tiny local vectors via match_jobs path by stubbing loader
    index = []
    from ai_gateway.router import ModelRouter
    from embedding_index import build_embedding_index

    router = ModelRouter()
    index = build_embedding_index(jobs[:80], router)

    hybrid_scores = []
    agent_scores = []

    with patch("api.match_service.load_jobs_and_index", return_value=(jobs, index)), patch(
        "agent.nodes.load_jobs_and_index", return_value=(jobs, index)
    ):
        for item in labels:
            query_kwargs = {
                "resume": item.get("resume_excerpt", ""),
                "dream_role": item["dream_role"],
                "location": item.get("location", ""),
                "limit": 10,
            }
            hybrid = match_jobs(**query_kwargs, method="hybrid")
            agent = run_match_agent(**query_kwargs)
            hybrid_scores.append(_citation_validity(hybrid.get("jobs") or []))
            agent_scores.append(_citation_validity(agent.get("jobs") or []))

    report = {
        "n_queries": len(labels),
        "hybrid_avg_citation_validity": round(sum(hybrid_scores) / len(hybrid_scores), 4),
        "agent_avg_citation_validity": round(sum(agent_scores) / len(agent_scores), 4),
        "note": "Local-provider ablation; Bedrock sample optional later for thesis appendix.",
    }
    out = ROOT / "evals" / "results" / "agent_ablation.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

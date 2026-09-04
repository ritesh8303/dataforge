"""Run enrichment quality evaluation vs regex baseline."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from enrichment.enricher import JobEnricher
from evals.metrics import skill_f1

SKILL_KEYWORDS = ["Python", "SQL", "AWS", "Spark", "LLM", "PyTorch", "Docker", "Kubernetes"]
skill_pattern = re.compile(r"\b(" + "|".join(re.escape(s) for s in SKILL_KEYWORDS) + r")\b", re.IGNORECASE)


def regex_skills(text: str) -> set[str]:
    return {m.group() for m in skill_pattern.finditer(text)}


def main():
    samples = [
        {
            "job_id": "t1",
            "title": "Data Engineer Python AWS",
            "company": "Co",
            "location": "Berlin",
            "description": "Build pipelines with Python, Spark, Airflow on AWS. Kubernetes experience a plus.",
            "gold_skills": {"Python", "AWS", "Spark"},
        },
        {
            "job_id": "t2",
            "title": "ML Engineer LLM",
            "company": "AI",
            "location": "Remote",
            "description": "Work on LLM fine-tuning with PyTorch and RAG pipelines.",
            "gold_skills": {"LLM", "PyTorch"},
        },
    ]

    enricher = JobEnricher()
    rows = []
    for s in samples:
        text = f"{s['title']} {s['description']}"
        ai = enricher.enrich_job(s)
        ai_skills = set(json.loads(ai["ai_skills"])) if ai.get("ai_skills") else set()
        regex = regex_skills(text)
        rows.append({
            "job_id": s["job_id"],
            "ai_f1": skill_f1(ai_skills, s["gold_skills"]),
            "regex_f1": skill_f1(regex, s["gold_skills"]),
            "json_valid": bool(ai.get("ai_seniority")),
        })

    out_dir = ROOT / "evals" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "avg_ai_f1": sum(r["ai_f1"]["f1"] for r in rows) / len(rows),
        "avg_regex_f1": sum(r["regex_f1"]["f1"] for r in rows) / len(rows),
        "json_validity_rate": sum(1 for r in rows if r["json_valid"]) / len(rows),
        "details": rows,
        "cost_summary": enricher.router.cost_logger.summary(),
    }
    out_path = out_dir / "enrichment_eval.json"
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()

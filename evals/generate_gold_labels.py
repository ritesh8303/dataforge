#!/usr/bin/env python3
"""Generate graded relevance labels from real Gold jobs (Tier 1 eval upgrade).

Uses deterministic field/seniority/skill overlap rules — not LLM — so CI stays
offline. Output: evals/data/gold_label_queries.json
"""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "data" / "gold" / "all_jobs.csv"
OUT = ROOT / "evals" / "data" / "gold_label_queries.json"

QUERIES = [
    {"query_id": "g_de_berlin", "dream_role": "Data Engineer Python AWS", "location": "Berlin", "field": "data_engineering"},
    {"query_id": "g_ws_analytics", "dream_role": "Werkstudent Data Analyst", "location": "", "field": "data_analytics", "seniority": "working_student"},
    {"query_id": "g_ml_intern", "dream_role": "Machine Learning Internship", "location": "", "field": "ai_ml_data_science", "seniority": "internship"},
    {"query_id": "g_thesis_ds", "dream_role": "Master Thesis Data Science", "location": "Germany", "field": "ai_ml_data_science", "seniority": "thesis"},
    {"query_id": "g_bi_junior", "dream_role": "Junior Business Intelligence Power BI", "location": "", "field": "business_intelligence"},
]


def _grade(job: dict, q: dict) -> int:
    field = (job.get("ai_field") or job.get("field") or "").lower()
    sen = (job.get("ai_seniority") or job.get("employment_type") or "").lower()
    title = (job.get("title") or "").lower()
    loc = (job.get("location") or "").lower()
    score = 0
    want_field = (q.get("field") or "").lower()
    if want_field and want_field in field:
        score += 2
    elif want_field and any(t in title for t in want_field.replace("_", " ").split()):
        score += 1
    want_sen = (q.get("seniority") or "").lower()
    if want_sen and want_sen in sen:
        score += 1
    if q.get("location") and q["location"].lower() in loc:
        score += 1
    # skill tokens from dream role
    for tok in re.findall(r"[a-zA-Z]{3,}", q.get("dream_role") or ""):
        if tok.lower() in title or tok.lower() in (job.get("tags") or "").lower():
            score += 1
            break
    if score >= 3:
        return 2
    if score >= 1:
        return 1
    return 0


def main() -> int:
    if not GOLD.exists():
        print(f"missing {GOLD} — skipping")
        return 0
    with GOLD.open(encoding="utf-8-sig", newline="") as fh:
        jobs = list(csv.DictReader(fh))
    labels = []
    for q in QUERIES:
        grades = {}
        for job in jobs:
            jid = str(job.get("job_id") or "")
            if not jid:
                continue
            g = _grade(job, q)
            if g:
                grades[jid] = g
        # keep top 20 relevant
        top = dict(sorted(grades.items(), key=lambda x: -x[1])[:20])
        labels.append(
            {
                **q,
                "resume_excerpt": f"Candidate for {q['dream_role']} in {q.get('location') or 'EU'}.",
                "relevant_job_ids": list(top.keys()),
                "relevance_grades": top,
                "source": "gold_deterministic",
            }
        )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(labels, indent=2), encoding="utf-8")
    print(f"wrote {len(labels)} queries -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

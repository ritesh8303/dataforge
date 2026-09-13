"""CI guard: matching eval fixtures must be large enough and non-saturating."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_eval_fixtures_meet_minimums():
    jobs = json.loads((ROOT / "evals" / "data" / "sample_jobs.json").read_text(encoding="utf-8"))
    queries = json.loads((ROOT / "evals" / "data" / "label_queries.json").read_text(encoding="utf-8"))
    labels = json.loads((ROOT / "evals" / "data" / "enrichment_labels.json").read_text(encoding="utf-8"))
    assert len(jobs) >= 80
    assert len(queries) >= 40
    assert len(labels) >= 100
    # Graded relevance present for harder nDCG
    assert any(q.get("relevance_grades") for q in queries)


def test_matching_eval_runs_and_not_saturated():
    result = subprocess.run(
        [sys.executable, str(ROOT / "evals" / "run_matching_eval.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    summary = json.loads((ROOT / "evals" / "results" / "matching_eval.json").read_text(encoding="utf-8"))
    ndcgs = [v["ndcg@10"] for v in summary["averages"].values()]
    assert not all(v >= 0.999 for v in ndcgs)


def test_enrichment_rules_eval_runs():
    result = subprocess.run(
        [sys.executable, str(ROOT / "evals" / "run_enrichment_rules_eval.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr

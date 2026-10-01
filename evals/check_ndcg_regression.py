#!/usr/bin/env python3
"""CI gate: fail if dense nDCG@10 regresses vs committed baseline."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVAL = ROOT / "evals" / "results" / "matching_eval.json"
BASE = ROOT / "evals" / "baselines" / "ndcg_baseline.json"


def main() -> int:
    if not EVAL.exists():
        print(f"MISSING {EVAL} — run evals/run_matching_eval.py first", file=sys.stderr)
        return 2
    if not BASE.exists():
        print(f"MISSING {BASE}", file=sys.stderr)
        return 2

    eval_payload = json.loads(EVAL.read_text(encoding="utf-8"))
    baseline = json.loads(BASE.read_text(encoding="utf-8"))
    method = baseline.get("method", "dense")
    metric = baseline.get("metric", "ndcg@10")
    base_val = float(baseline["baseline"])
    max_drop = float(baseline.get("max_drop", 0.02))

    averages = eval_payload.get("averages") or {}
    current = float((averages.get(method) or {}).get(metric, 0.0))
    drop = base_val - current
    print(f"{method} {metric}: current={current:.4f} baseline={base_val:.4f} drop={drop:.4f} max_drop={max_drop}")
    if drop > max_drop + 1e-9:
        print("REGRESSION: nDCG gate failed", file=sys.stderr)
        return 1
    print("nDCG gate OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

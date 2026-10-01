"""Run the ingest-review agent against data/hitl_ingest_review_queue.csv.

Examples:
  py -3 scripts/run_ingest_review_agent.py
  py -3 scripts/run_ingest_review_agent.py --llm --max-llm 20
  py -3 scripts/run_ingest_review_agent.py --limit 50 --no-llm
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest HITL review agent")
    parser.add_argument("--queue", default="", help="Path to hitl_ingest_review_queue.csv")
    parser.add_argument("--decisions", default="", help="Path to write decisions CSV")
    parser.add_argument("--llm", action="store_true", help="Use LLM for remaining ambiguous rows")
    parser.add_argument("--no-llm", action="store_true", help="Rules-only (default)")
    parser.add_argument("--max-llm", type=int, default=40)
    parser.add_argument("--limit", type=int, default=0, help="Max queue rows to process (0=all)")
    parser.add_argument("--redo", action="store_true", help="Re-decide even if already decided")
    args = parser.parse_args()

    use_llm = bool(args.llm) and not bool(args.no_llm)
    if use_llm:
        os.environ.setdefault("AI_ENABLED", "true")

    from agent.ingest_review_agent import run_ingest_review_agent

    summary = run_ingest_review_agent(
        queue_path=args.queue or None,
        decisions_path=args.decisions or None,
        use_llm=use_llm,
        max_llm=args.max_llm,
        limit=args.limit or None,
        skip_decided=not args.redo,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

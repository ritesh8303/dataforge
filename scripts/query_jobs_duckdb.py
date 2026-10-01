#!/usr/bin/env python3
"""CLI: DuckDB search over Gold all_jobs.csv."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from api.duckdb_jobs import search_jobs_duckdb  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--query", default="")
    p.add_argument("--location", default="")
    p.add_argument("--field", default="")
    p.add_argument("--employment", default="")
    p.add_argument("--limit", type=int, default=20)
    args = p.parse_args()
    try:
        result = search_jobs_duckdb(
            query=args.query,
            location=args.location,
            field=args.field,
            employment=args.employment,
            limit=args.limit,
        )
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        return 1
    print(json.dumps({"count": result["count"], "backend": result["backend"]}, indent=2))
    for job in result["jobs"][: args.limit]:
        print(f"- {job.get('title')} | {job.get('company')} | {job.get('location')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

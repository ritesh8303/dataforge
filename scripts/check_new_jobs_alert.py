#!/usr/bin/env python3
"""Alert on new product-board jobs (config/saved_searches.json or CLI filters).

Usage:
  py -3 scripts/check_new_jobs_alert.py --dry-run
  py -3 scripts/check_new_jobs_alert.py --field data_engineering --city Berlin
  set ALERT_WEBHOOK_URL=https://... && py -3 scripts/check_new_jobs_alert.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from alerts.saved_search import SavedSearch, run_alerts  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--field", default="")
    parser.add_argument("--city", default="")
    parser.add_argument("--employment", default="")
    parser.add_argument("--keywords", default="")
    parser.add_argument("--name", default="cli")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--use-config", action="store_true", help="Load config/saved_searches.json")
    args = parser.parse_args()

    searches = None
    if args.use_config or not (args.field or args.city or args.employment or args.keywords):
        searches = None  # run_alerts loads config; if empty falls back
        if args.field or args.city or args.employment or args.keywords:
            searches = [
                SavedSearch(
                    name=args.name,
                    field=args.field,
                    city=args.city,
                    employment=args.employment,
                    keywords=args.keywords,
                )
            ]
        else:
            searches = None  # use config file
    else:
        searches = [
            SavedSearch(
                name=args.name,
                field=args.field,
                city=args.city,
                employment=args.employment,
                keywords=args.keywords,
            )
        ]

    # If CLI filters given, always use them; else config
    if args.field or args.city or args.employment or args.keywords:
        searches = [
            SavedSearch(
                name=args.name,
                field=args.field,
                city=args.city,
                employment=args.employment,
                keywords=args.keywords,
            )
        ]

    result = run_alerts(searches=searches, dry_run=args.dry_run)
    print(
        f"scanned={result['scanned']} seen={result['seen']} "
        f"searches={result['searches']} new_total={result['new_total']}"
    )
    for block in result["results"]:
        print(f"[{block['search']['name']}] matches={block['count']}")
        for j in block["jobs"][:10]:
            print(f"- {j.get('title')} | {j.get('company')} | {j.get('location')} | {j.get('url')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

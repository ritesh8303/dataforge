#!/usr/bin/env python3
"""Alert on new product-board jobs matching optional filters.

Usage:
  py -3 scripts/check_new_jobs_alert.py --field data_engineering --city Berlin
  set ALERT_WEBHOOK_URL=https://... && py -3 scripts/check_new_jobs_alert.py
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SEEN = ROOT / "data" / "alert_seen_job_ids.json"
LOCAL_GOLD = ROOT / "data" / "gold" / "all_jobs.csv"


def _load_jobs() -> list[dict]:
    bucket = os.environ.get("GOLD_BUCKET", "").strip()
    if bucket:
        import boto3
        from io import StringIO

        obj = boto3.client("s3").get_object(Bucket=bucket, Key="all_jobs.csv")
        return list(csv.DictReader(StringIO(obj["Body"].read().decode("utf-8-sig"))))
    if LOCAL_GOLD.exists():
        with LOCAL_GOLD.open(encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))
    raise FileNotFoundError("No local data/gold/all_jobs.csv and GOLD_BUCKET unset")


def _match(job: dict, *, field: str, city: str, employment: str) -> bool:
    if field:
        f = (job.get("ai_field") or job.get("field") or "").lower()
        if field.lower() not in f and field.lower().replace("_", " ") not in (job.get("title") or "").lower():
            return False
    if city and city.lower() not in (job.get("location") or "").lower():
        return False
    if employment:
        emp = (job.get("employment_type") or job.get("ai_seniority") or "").lower()
        if employment.lower() not in emp:
            return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--field", default="")
    parser.add_argument("--city", default="")
    parser.add_argument("--employment", default="")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    jobs = _load_jobs()
    seen = set()
    if SEEN.exists():
        seen = set(json.loads(SEEN.read_text(encoding="utf-8")).get("job_ids") or [])

    fresh = []
    for job in jobs:
        jid = str(job.get("job_id") or "")
        if not jid or jid in seen:
            continue
        if _match(job, field=args.field, city=args.city, employment=args.employment):
            fresh.append(job)

    print(f"scanned={len(jobs)} seen={len(seen)} new_matches={len(fresh)}")
    for j in fresh[:20]:
        print(f"- {j.get('title')} | {j.get('company')} | {j.get('location')} | {j.get('job_url') or j.get('url')}")

    webhook = os.environ.get("ALERT_WEBHOOK_URL", "").strip()
    if webhook and fresh and not args.dry_run:
        body = {
            "text": f"DataForge: {len(fresh)} new early-career jobs"
            + (f" ({args.field or 'any field'})" if args.field or args.city else ""),
            "jobs": [
                {"title": j.get("title"), "company": j.get("company"), "url": j.get("job_url") or j.get("url")}
                for j in fresh[:15]
            ],
        }
        req = urllib.request.Request(
            webhook,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=30)
        print(f"webhook_posted {webhook[:48]}...")

    if not args.dry_run:
        all_ids = sorted({str(j.get("job_id")) for j in jobs if j.get("job_id")} | seen)
        SEEN.parent.mkdir(parents=True, exist_ok=True)
        SEEN.write_text(json.dumps({"job_ids": all_ids}, indent=2), encoding="utf-8")
        print(f"updated {SEEN}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

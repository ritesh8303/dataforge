"""Ingest remote data/AI-adjacent jobs from Remotive public API."""

from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime, timezone

import pandas as pd
import requests

API_URL = "https://remotive.com/api/remote-jobs"
SOURCE = "remotive"
ATTRIBUTION = "https://remotive.com (public API)"

# Soft gate: keep data/AI-ish or general software roles for lakehouse; audience gate does product filter.
_DATA_AI_RE = re.compile(
    r"\b(data|analytics|analyst|scientist|machine\s*learning|\bml\b|\bai\b|llm|"
    r"etl|spark|dbt|bi\b|business\s*intelligence|mlops|python)\b",
    re.I,
)


def fetch_remotive_jobs(categories: list[str] | None = None) -> list[dict]:
    categories = categories or ["data", "software-dev"]
    headers = {
        "User-Agent": "DataForge Job Aggregator/1.0 (+https://github.com/ritesh8303/dataforge)",
        "Accept": "application/json",
    }
    jobs: list[dict] = []
    seen: set[str] = set()
    for category in categories:
        resp = requests.get(API_URL, params={"category": category}, headers=headers, timeout=45)
        resp.raise_for_status()
        payload = resp.json()
        batch = payload.get("jobs") or []
        for item in batch:
            title = str(item.get("title") or "").strip()
            company = str(item.get("company_name") or "").strip()
            url = str(item.get("url") or "").strip()
            if not title or not company:
                continue
            blob = f"{title} {item.get('description') or ''} {item.get('tags') or ''}"
            if category == "software-dev" and not _DATA_AI_RE.search(blob):
                continue
            jid = str(item.get("id") or hashlib.sha256(f"{company}|{title}|{url}".encode()).hexdigest()[:16])
            if jid in seen:
                continue
            seen.add(jid)
            tags = item.get("tags") or []
            if isinstance(tags, list):
                tags_s = ",".join(str(t) for t in tags if t)
            else:
                tags_s = str(tags)
            jobs.append(
                {
                    "job_id": f"rem_{jid}",
                    "title": title,
                    "company": company,
                    "location": str(item.get("candidate_required_location") or "Worldwide Remote"),
                    "url": url,
                    "description": str(item.get("description") or "")[:5000],
                    "remote": True,
                    "tags": tags_s,
                    "job_types": str(item.get("job_type") or "full_time"),
                    "salary": str(item.get("salary") or ""),
                    "published_at": str(item.get("publication_date") or ""),
                    "source": SOURCE,
                    "source_attribution": ATTRIBUTION,
                }
            )
    return jobs


def lambda_handler(event, context):
    bucket = os.environ.get("BRONZE_BUCKET")
    is_local = os.environ.get("LOCAL_RUN") == "true"
    if not bucket and not is_local:
        raise ValueError("BRONZE_BUCKET environment variable is not set.")

    rows = fetch_remotive_jobs()
    if not rows:
        print("No jobs found from Remotive.")
        return {"statusCode": 204, "body": "No jobs found to ingest."}

    df = pd.DataFrame(rows)
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = f"s3://{bucket}/remotive/ingested_at={date_str}/jobs.parquet"

    from processing.utils import save_parquet

    save_parquet(df, path, SOURCE)
    print(f"Successfully ingested {len(df)} jobs from Remotive.")
    return {"statusCode": 200, "body": f"Successfully ingested {len(df)} jobs."}

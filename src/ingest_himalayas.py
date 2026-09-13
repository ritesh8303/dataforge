"""Ingest remote/entry-level jobs for Germany from Himalayas public API."""

from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone

import pandas as pd
import requests

API_URL = "https://himalayas.app/jobs/api/search"
SOURCE = "himalayas"
ATTRIBUTION = "https://himalayas.app (link back required by Himalayas API terms)"


def fetch_himalayas_jobs(
    country: str = "DE",
    seniority: str | None = "Entry-level",
    max_pages: int = 5,
) -> list[dict]:
    headers = {
        "User-Agent": "DataForge Job Aggregator/1.0 (+https://github.com/dataforge)",
        "Accept": "application/json",
    }
    jobs: list[dict] = []
    for page in range(1, max_pages + 1):
        params: dict[str, str | int] = {"country": country, "page": page}
        if seniority:
            params["seniority"] = seniority
        resp = requests.get(API_URL, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        batch = payload.get("jobs") or payload.get("data") or []
        if isinstance(payload, list):
            batch = payload
        if not batch:
            break
        for item in batch:
            title = str(item.get("title") or item.get("jobTitle") or "").strip()
            company = str(item.get("companyName") or item.get("company") or "").strip()
            url = str(item.get("applicationLink") or item.get("url") or item.get("guid") or "").strip()
            if not title or not company:
                continue
            loc = item.get("location") or item.get("locations") or country
            if isinstance(loc, list):
                loc = ", ".join(str(x) for x in loc)
            description = str(item.get("description") or item.get("excerpt") or "")
            jid = str(item.get("id") or hashlib.sha256(f"{company}|{title}|{url}".encode()).hexdigest()[:16])
            jobs.append(
                {
                    "job_id": f"him_{jid}",
                    "title": title,
                    "company": company,
                    "location": str(loc),
                    "url": url or f"https://himalayas.app/jobs/{jid}",
                    "description": description,
                    "remote": True,
                    "tags": ",".join(
                        str(t) for t in (item.get("categories") or item.get("tags") or []) if t
                    ),
                    "job_types": str(item.get("employmentType") or item.get("type") or "full_time"),
                    "source": SOURCE,
                    "source_attribution": ATTRIBUTION,
                }
            )
        # Stop if API indicates last page
        if len(batch) < 10:
            break
    return jobs


def lambda_handler(event, context):
    bucket = os.environ.get("BRONZE_BUCKET")
    is_local = os.environ.get("LOCAL_RUN") == "true"
    if not bucket and not is_local:
        raise ValueError("BRONZE_BUCKET environment variable is not set.")

    rows = fetch_himalayas_jobs()
    if not rows:
        print("No jobs found from Himalayas.")
        return {"statusCode": 204, "body": "No jobs found to ingest."}

    df = pd.DataFrame(rows)
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = f"s3://{bucket}/himalayas/ingested_at={date_str}/jobs.parquet"

    from processing.utils import save_parquet

    save_parquet(df, path, SOURCE)
    print(f"Successfully ingested {len(df)} jobs from Himalayas.")
    return {"statusCode": 200, "body": f"Successfully ingested {len(df)} jobs."}

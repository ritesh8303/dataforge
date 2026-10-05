"""Ingest remote jobs from Jobicy public API (worldwide)."""

from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime, timezone

import pandas as pd
import requests

API_URL = "https://jobicy.com/api/v2/remote-jobs"
SOURCE = "jobicy"
ATTRIBUTION = "https://jobicy.com (public API)"

_DATA_AI_RE = re.compile(
    r"\b(data|analytics|analyst|scientist|machine\s*learning|\bml\b|\bai\b|llm|"
    r"etl|spark|dbt|bi\b|business\s*intelligence|mlops|python|devops|cloud)\b",
    re.I,
)


def fetch_jobicy_jobs(count: int = 50, tags: list[str] | None = None) -> list[dict]:
    tags = tags or ["data", "devops", "software-engineering"]
    headers = {
        "User-Agent": "DataForge Job Aggregator/1.0 (+https://github.com/ritesh8303/dataforge)",
        "Accept": "application/json",
    }
    jobs: list[dict] = []
    seen: set[str] = set()
    for tag in tags:
        resp = requests.get(
            API_URL,
            params={"count": count, "tag": tag},
            headers=headers,
            timeout=45,
        )
        resp.raise_for_status()
        payload = resp.json()
        batch = payload.get("jobs") or payload.get("data") or []
        for item in batch:
            title = str(item.get("jobTitle") or item.get("title") or "").strip()
            company = str(item.get("companyName") or item.get("company") or "").strip()
            url = str(item.get("url") or item.get("jobUrl") or "").strip()
            if not title or not company:
                continue
            desc = str(item.get("jobDescription") or item.get("description") or "")
            blob = f"{title} {desc} {item.get('jobIndustry') or ''}"
            if not _DATA_AI_RE.search(blob):
                continue
            jid = str(
                item.get("id")
                or hashlib.sha256(f"{company}|{title}|{url}".encode()).hexdigest()[:16]
            )
            if jid in seen:
                continue
            seen.add(jid)
            loc = item.get("jobGeo") or item.get("location") or "Worldwide Remote"
            if isinstance(loc, list):
                loc = ", ".join(str(x) for x in loc)
            jobs.append(
                {
                    "job_id": f"jcy_{jid}",
                    "title": title,
                    "company": company,
                    "location": str(loc),
                    "url": url,
                    "description": desc[:5000],
                    "remote": True,
                    "tags": str(item.get("jobIndustry") or tag),
                    "job_types": str(item.get("jobType") or "full_time"),
                    "salary": str(item.get("annualSalaryMin") or item.get("salary") or ""),
                    "published_at": str(item.get("pubDate") or ""),
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

    rows = fetch_jobicy_jobs()
    if not rows:
        print("No jobs found from Jobicy.")
        return {"statusCode": 204, "body": "No jobs found to ingest."}

    df = pd.DataFrame(rows)
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = f"s3://{bucket}/jobicy/ingested_at={date_str}/jobs.parquet"

    from processing.utils import save_parquet

    save_parquet(df, path, SOURCE)
    print(f"Successfully ingested {len(df)} jobs from Jobicy.")
    return {"statusCode": 200, "body": f"Successfully ingested {len(df)} jobs."}

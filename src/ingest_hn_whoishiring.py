"""Ingest Germany/EU tech roles from Hacker News 'Who is hiring' via Algolia.

Uses source id `hn_whoishiring` (distinct from legacy purged `hacker_news`).
"""

from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime, timezone

import pandas as pd
import requests

SOURCE = "hn_whoishiring"
ATTRIBUTION = "Hacker News / Algolia HN Search API"
ALGOLIA_URL = "https://hn.algolia.com/api/v1/search_by_date"

# Keep only posts that look EU/DE-relevant.
_LOCATION_RE = re.compile(
    r"\b("
    r"germany|berlin|munich|m[uü]nchen|hamburg|cologne|k[oö]ln|"
    r"frankfurt|stuttgart|d[uü]sseldorf|amsterdam|remote[\s-]*eu|"
    r"eu[\s-]*remote|europe|netherlands|austria|switzerland"
    r")\b",
    re.I,
)
_TECH_RE = re.compile(
    r"\b("
    r"engineer|developer|data|ml|ai|backend|frontend|devops|sre|"
    r"python|java|golang|typescript|kubernetes|aws|software"
    r")\b",
    re.I,
)


def _parse_hn_line(text: str) -> dict[str, str]:
    """Best-effort parse of 'Company | Role | Location | …' hiring comments."""
    parts = [p.strip() for p in re.split(r"\s*[|•·]\s*", text) if p.strip()]
    company = parts[0][:120] if parts else "Unknown"
    title = parts[1][:200] if len(parts) > 1 else (text[:120] or "Software Engineer")
    location = ""
    for part in parts[2:6]:
        if _LOCATION_RE.search(part):
            location = part[:120]
            break
    if not location:
        m = _LOCATION_RE.search(text)
        location = m.group(0) if m else "Remote / EU"
    return {"company": company, "title": title, "location": location}


def fetch_hn_whoishiring(max_hits: int = 200) -> list[dict]:
    headers = {"User-Agent": "DataForge Job Aggregator/1.0"}
    params = {
        "query": "Who is hiring",
        "tags": "story,author_whoishiring",
        "hitsPerPage": 5,
    }
    # Find recent monthly threads, then pull comments.
    threads = requests.get(ALGOLIA_URL, params=params, headers=headers, timeout=30)
    threads.raise_for_status()
    story_ids = [h.get("objectID") for h in threads.json().get("hits", []) if h.get("objectID")]

    jobs: list[dict] = []
    seen = set()
    for story_id in story_ids:
        page = 0
        while len(jobs) < max_hits:
            resp = requests.get(
                ALGOLIA_URL,
                params={
                    "tags": f"comment,story_{story_id}",
                    "hitsPerPage": 100,
                    "page": page,
                },
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            hits = resp.json().get("hits", [])
            if not hits:
                break
            for hit in hits:
                text = re.sub(r"<[^>]+>", " ", hit.get("comment_text") or "")
                text = re.sub(r"\s+", " ", text).strip()
                if not text or not _LOCATION_RE.search(text) or not _TECH_RE.search(text):
                    continue
                parsed = _parse_hn_line(text)
                key = f"{parsed['company']}|{parsed['title']}|{parsed['location']}"
                if key in seen:
                    continue
                seen.add(key)
                oid = hit.get("objectID") or hashlib.sha256(key.encode()).hexdigest()[:12]
                jobs.append(
                    {
                        "job_id": f"hn_{oid}",
                        "title": parsed["title"],
                        "company": parsed["company"],
                        "location": parsed["location"],
                        "url": f"https://news.ycombinator.com/item?id={oid}",
                        "description": text[:3000],
                        "remote": bool(re.search(r"\bremote\b", text, re.I)),
                        "tags": "hackernews,tech",
                        "job_types": "full_time",
                        "source": SOURCE,
                        "source_attribution": ATTRIBUTION,
                    }
                )
                if len(jobs) >= max_hits:
                    break
            page += 1
            if page > 10:
                break
    return jobs


def lambda_handler(event, context):
    bucket = os.environ.get("BRONZE_BUCKET")
    is_local = os.environ.get("LOCAL_RUN") == "true"
    if not bucket and not is_local:
        raise ValueError("BRONZE_BUCKET environment variable is not set.")

    rows = fetch_hn_whoishiring()
    if not rows:
        print("No jobs found from HN Who is hiring.")
        return {"statusCode": 204, "body": "No jobs found to ingest."}

    df = pd.DataFrame(rows)
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = f"s3://{bucket}/hn_whoishiring/ingested_at={date_str}/jobs.parquet"

    from processing.utils import save_parquet

    save_parquet(df, path, SOURCE)
    print(f"Successfully ingested {len(df)} jobs from HN Who is hiring.")
    return {"statusCode": 200, "body": f"Successfully ingested {len(df)} jobs."}

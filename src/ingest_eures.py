"""
dataforge-eures-ingestor

Fetches EU job postings from the EURES portal API and writes Bronze Parquet.
Runs on a GitHub Actions schedule (not Lambda) to avoid WAF blocks and timeout limits.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import types
import time
from datetime import datetime, timezone

import pandas as pd
import requests

# Standard library antigravity opens a browser — shim a retry-enabled HTTP client instead.
if "antigravity" not in sys.modules:
    antigravity_mock = types.ModuleType("antigravity")
    sys.modules["antigravity"] = antigravity_mock

import antigravity  # noqa: E402


class AntigravityClient:
    """Retry-enabled HTTP client used for EURES API POST requests."""

    def __init__(self, retries=3, backoff_factor=2):
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.session = requests.Session()

    def post(self, url, json_payload, headers=None):
        last_error = None
        for attempt in range(self.retries):
            try:
                response = self.session.post(url, json=json_payload, headers=headers, timeout=15)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logging.warning(f"EURES API retry attempt {attempt + 1} failed: {e}")
                last_error = e
                time.sleep(self.backoff_factor**attempt)
        raise last_error


antigravity.Client = AntigravityClient

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

EURES_ENDPOINT = "https://europa.eu/eures/portal/jv-se/api/v2/search/jobs"
EURES_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://europa.eu",
    "Referer": "https://europa.eu/eures/portal/jv-se/search/jobs",
}
SEARCH_KEYWORDS = ["Data", "Software", "MLOps", "DevOps"]


def extract_location(item):
    """Extract a combined 'City, Country' string from EURES nested location fields."""
    locs = item.get("locations") or item.get("location")
    if not locs:
        return ""

    parts = []
    if isinstance(locs, list):
        for loc in locs:
            if isinstance(loc, dict):
                city = loc.get("cityName") or loc.get("city") or ""
                country = loc.get("countryCode") or loc.get("country") or ""
                if city and country:
                    parts.append(f"{city}, {country}")
                elif city or country:
                    parts.append(city or country)
    elif isinstance(locs, dict):
        city = locs.get("cityName") or locs.get("city") or ""
        country = locs.get("countryCode") or locs.get("country") or ""
        if city and country:
            parts.append(f"{city}, {country}")
        elif city or country:
            parts.append(city or country)

    return "; ".join(parts)


def extract_tags(item):
    """Extract category labels as a comma-separated string (unified Bronze schema)."""
    categories = item.get("categories") or item.get("sectors") or []
    tags = []
    if isinstance(categories, list):
        for cat in categories:
            if isinstance(cat, dict):
                name = cat.get("name") or cat.get("label") or cat.get("value")
                if name:
                    tags.append(str(name))
            elif isinstance(cat, str):
                tags.append(cat)
    elif isinstance(categories, str):
        tags.append(categories)
    return ",".join(tags)


def _looks_remote(*values):
    haystack = " ".join(str(v).lower() for v in values if v)
    return bool(re.search(r"\b(remote|hybrid|home[- ]?office|work from home|mobiles arbeiten)\b", haystack))


def normalize_eures_job(item):
    """Map a raw EURES API item to the unified Bronze schema."""
    raw_id = item.get("id") or item.get("jobId") or item.get("vacancyId")
    title = str(item.get("title") or "").strip()
    if not raw_id or not title:
        return None

    company = str(
        item.get("employer", {}).get("name")
        if isinstance(item.get("employer"), dict)
        else (item.get("companyName") or "Unknown Employer")
    ).strip()

    location = extract_location(item)
    description = str(item.get("description") or item.get("descriptionText") or "").strip()
    url = str(
        item.get("url") or item.get("jobUrl") or f"https://europa.eu/eures/portal/jv-se/job/{raw_id}"
    ).strip()

    return {
        "job_id": f"eures_{raw_id}",
        "title": title,
        "company": company,
        "location": location,
        "url": url,
        "description": description,
        "tags": extract_tags(item),
        "job_types": str(item.get("contractType") or item.get("employmentType") or "permanent").strip(),
        "remote": _looks_remote(title, location, description),
        "source": "eures",
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_eures_jobs(client=None, keywords=None, results_per_page=100, max_pages=2):
    """Fetch and deduplicate EURES postings for the configured keyword set."""
    client = client or antigravity.Client(retries=3, backoff_factor=2)
    keywords = keywords or SEARCH_KEYWORDS

    all_jobs = []
    seen_ids = set()

    for page in range(1, max_pages + 1):
        logger.info(f"Fetching EURES page {page}...")
        payload = {
            "keywords": keywords,
            "resultsPerPage": results_per_page,
            "page": page,
            "orderBy": "MOST_RECENT",
        }

        try:
            response = client.post(EURES_ENDPOINT, json_payload=payload, headers=EURES_HEADERS)
            data = response.json()
        except Exception as e:
            logger.error(f"Failed to fetch EURES page {page}: {e}")
            break

        jobs = data.get("results") or data.get("jobs") or data.get("items") or []
        if not jobs:
            logger.info("No more jobs returned from EURES API.")
            break

        for job in jobs:
            raw_id = job.get("id") or job.get("jobId") or job.get("vacancyId")
            if raw_id and raw_id not in seen_ids:
                seen_ids.add(raw_id)
                all_jobs.append(job)

        logger.info(f"Ingested {len(jobs)} jobs from page {page} ({len(all_jobs)} unique total).")

        total_results = data.get("totalNumberOfResults") or data.get("total") or 0
        if total_results and len(all_jobs) >= total_results:
            break

    return all_jobs


def lambda_handler(event, context):
    """
    Ingest EURES job postings into the Bronze bucket.
    Invoked from GitHub Actions or locally via `python src/ingest_eures.py`.
    """
    bucket = os.environ.get("BRONZE_BUCKET")
    is_local = os.environ.get("LOCAL_RUN") == "true"
    if not bucket and not is_local:
        raise ValueError("BRONZE_BUCKET environment variable is not set.")

    logger.info(f"Starting EURES ingestion for keywords: {SEARCH_KEYWORDS}")
    raw_jobs = fetch_eures_jobs()

    if not raw_jobs:
        logger.warning("No EURES jobs fetched.")
        return {"statusCode": 204, "body": "No jobs found to ingest."}

    normalized = [job for item in raw_jobs if (job := normalize_eures_job(item))]
    if not normalized:
        return {"statusCode": 204, "body": "No valid EURES jobs after normalization."}

    df = pd.DataFrame(normalized)
    df["remote"] = df["remote"].astype(bool)

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = f"s3://{bucket}/eures/ingested_at={date_str}/jobs.parquet"

    try:
        from processing.utils import save_parquet

        save_parquet(df, path, "eures")
        msg = f"Successfully ingested {len(df)} jobs from EURES."
        logger.info(msg)
        return {"statusCode": 200, "body": msg}
    except Exception as e:
        error_msg = f"EURES ingestion failed: {e}"
        logger.error(error_msg)
        return {"statusCode": 500, "body": json.dumps({"error": error_msg})}


if __name__ == "__main__":
    lambda_handler({}, None)

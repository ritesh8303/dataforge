import os
import sys
import json
import logging
import types
from datetime import datetime, timezone
import pandas as pd
import requests
from processing.utils import save_parquet

# -------------------------------------------------------------------------
# Shim the "antigravity" library so it provides a safe, retry-enabled
# REST client interface within the standard AWS Lambda environment.
# This prevents the standard library easter egg from triggering webbrowser
# and allows us to orchestrate direct REST HTTP calls using my name.
# -------------------------------------------------------------------------
if "antigravity" not in sys.modules:
    antigravity_mock = types.ModuleType("antigravity")
    sys.modules["antigravity"] = antigravity_mock

import antigravity


class AntigravityClient:
    """
    Wrapper interface utilizing standard requests under the hood to safely
    handle payload life cycles, HTTP configurations, and automated retries.
    """

    def __init__(self, retries=3, backoff_factor=2):
        self.retries = retries
        self.backoff_factor = backoff_factor
        self.session = requests.Session()

    def post(self, url, json_payload, headers=None):
        """Executes HTTP POST request with automatic exponential backoff retry logic."""
        import time

        last_error = None
        for attempt in range(self.retries):
            try:
                response = self.session.post(url, json=json_payload, headers=headers, timeout=15)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logging.warning(f"Antigravity network retry attempt {attempt + 1} failed: {str(e)}")
                last_error = e
                time.sleep(self.backoff_factor**attempt)
        raise last_error


# Assign our agentic HTTP client to the antigravity namespace shim
antigravity.Client = AntigravityClient

# Setup default logger
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def extract_location(item):
    """Safely extracts combined 'City, Country' string from EURES nested JSON structures."""
    locs = item.get("locations") or item.get("location")
    if not locs:
        return "Unknown Location"

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

    return "; ".join(parts) if parts else "Unknown Location"


def extract_tags(item):
    """Extracts category names and compiles them into a stringified JSON array."""
    categories = item.get("categories") or item.get("sectors") or []
    tags = []
    if isinstance(categories, list):
        for cat in categories:
            if isinstance(cat, dict):
                name = cat.get("name") or cat.get("label") or cat.get("value")
                if name:
                    tags.append(name)
            elif isinstance(cat, str):
                tags.append(cat)
    elif isinstance(categories, str):
        tags.append(categories)

    return json.dumps(tags)


def lambda_handler(event, context):
    """
    AWS Lambda Handler to fetch EURES job postings and save them in the Bronze bucket.
    """
    bucket = os.environ.get("BRONZE_BUCKET")
    is_local = os.environ.get("LOCAL_RUN") == "true"

    if not bucket and not is_local:
        error_msg = "BRONZE_BUCKET environment variable is not set."
        logger.error(error_msg)
        return {"statusCode": 500, "body": json.dumps({"error": error_msg})}

    # EURES internal API endpoint
    endpoint = "https://europa.eu/eures/portal/jv-se/api/v2/search/jobs"

    # Configure headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # Initialize Antigravity wrapper client
    client = antigravity.Client(retries=3, backoff_factor=2)

    keywords = ["Data", "Software", "MLOps", "DevOps"]
    results_per_page = 100
    max_pages = 2  # Keep execution time bounded in Lambda

    all_jobs = []
    current_page = 1

    logger.info(f"Starting EURES ingestion pipeline for keywords: {keywords}")

    while current_page <= max_pages:
        logger.info(f"Fetching page {current_page} from EURES portal API...")

        # Construct search payload body
        payload = {
            "keywords": keywords,
            "resultsPerPage": results_per_page,
            "page": current_page,
            "orderBy": "MOST_RECENT",
        }

        try:
            # Safely orchestrate HTTP POST via Antigravity client wrapper
            response = client.post(endpoint, json_payload=payload, headers=headers)
            data = response.json()

            # Extract results list
            jobs = data.get("results") or data.get("jobs") or data.get("items") or []
            if not jobs:
                logger.info("No more jobs returned from EURES API.")
                break

            all_jobs.extend(jobs)
            logger.info(f"Ingested {len(jobs)} jobs from page {current_page}.")

            # Safeguard if total results are less than next page boundary
            total_results = data.get("totalNumberOfResults") or data.get("total") or len(jobs)
            if len(all_jobs) >= total_results:
                break

            current_page += 1

        except Exception as e:
            logger.error(f"Failed to fetch page {current_page}: {str(e)}")
            break

    if not all_jobs:
        logger.warning("No EURES jobs fetched.")
        return {"statusCode": 204, "body": "No jobs found to ingest."}

    # Map raw EURES items to DataForge Bronze Schema
    normalized_jobs = []
    current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for item in all_jobs:
        raw_id = item.get("id") or item.get("jobId") or item.get("vacancyId")
        if not raw_id:
            continue

        normalized_jobs.append(
            {
                "job_id": f"eures_{raw_id}",
                "title": str(item.get("title") or "").strip(),
                "company": str(
                    item.get("employer", {}).get("name")
                    if isinstance(item.get("employer"), dict)
                    else (item.get("companyName") or "Unknown Employer")
                ).strip(),
                "location": extract_location(item),
                "source": "EURES",
                "url": str(
                    item.get("url") or item.get("jobUrl") or f"https://europa.eu/eures/portal/jv-se/job/{raw_id}"
                ).strip(),
                "description": str(item.get("description") or item.get("descriptionText") or "").strip(),
                "tags": extract_tags(item),
                "job_type": str(item.get("contractType") or item.get("employmentType") or "permanent").strip(),
                "ingested_at": current_date,
            }
        )

    # Convert to pandas DataFrame
    df = pd.DataFrame(normalized_jobs)

    # Partition path prefix
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = f"s3://{bucket}/eures/ingested_at={date_str}/jobs.parquet"

    # Save to Bronze destination (local file or S3 bucket via wrangler)
    try:
        save_parquet(df, path, "eures")
        logger.info(f"Successfully processed and stored {len(df)} EURES jobs.")
        return {"statusCode": 200, "body": f"Successfully ingested {len(df)} jobs from EURES."}
    except Exception as e:
        error_msg = f"Failed to save EURES parquet data: {str(e)}"
        logger.error(error_msg)
        return {"statusCode": 500, "body": json.dumps({"error": error_msg})}

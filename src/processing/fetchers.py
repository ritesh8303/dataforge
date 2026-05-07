import os
import requests
from typing import Any, Dict
from .typing_inspection.arbeitnow import validate_api_response as validate_arbeitnow

class ArbeitnowFetcher:
    """Fetcher for the Arbeitnow public job board API."""

    API_URL = "https://www.arbeitnow.com/api/job-board-api"
    MAX_PAGES = 10

    def fetch_jobs(self) -> Dict[str, Any]:
        print("Fetching data from Arbeitnow API...")
        all_jobs = []
        page = 1

        while page <= self.MAX_PAGES:
            response = requests.get(self.API_URL, params={"page": page}, timeout=10)
            response.raise_for_status()
            raw_data = response.json()
            validated = validate_arbeitnow(raw_data)
            jobs = validated.get('data', [])
            if not jobs:
                break
            all_jobs.extend(jobs)
            # Stop if there are no more pages
            next_page = validated.get('links', {}).get('next')
            if not next_page:
                break
            page += 1

        print(f"Fetched {len(all_jobs)} total jobs from Arbeitnow across {page} page(s).")
        return {'data': all_jobs}

class BAFetcher:
    """Fetcher for the Bundesagentur fur Arbeit public Jobsuche API (no auth required)."""

    JOBS_URL = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/jobs"

    def __init__(self, ssm_parameter_name: str = None):
        pass  # No credentials needed for public API

    def fetch_jobs(self, query: str = "Data Engineer") -> Dict[str, Any]:
        """Fetches all pages of jobs from BA public API."""
        headers = {
            "X-API-Key": "jobboerse-jobsuche",
            "Accept": "application/json"
        }

        all_jobs = []
        page = 1
        page_size = 100

        while True:
            params = {"was": query, "size": page_size, "page": page}
            print(f"Fetching BA API page {page} for query: {query}")
            response = requests.get(self.JOBS_URL, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            raw_data = response.json()
            jobs = raw_data.get('stellenangebote', [])
            if not jobs:
                break
            all_jobs.extend(jobs)
            max_results = raw_data.get('maxErgebnisse', 0)
            if len(all_jobs) >= max_results:
                break
            page += 1

        print(f"Fetched {len(all_jobs)} total jobs from BA API for '{query}'.")
        return {'stellenangebote': all_jobs}

def get_fetcher(source: str) -> Any:
    """Factory function to get the appropriate fetcher."""
    if source == "arbeitnow":
        return ArbeitnowFetcher()
    elif source == "ba":
        return BAFetcher()
    else:
        raise ValueError(f"Unknown source: {source}")
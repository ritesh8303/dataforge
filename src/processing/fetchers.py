import json
import os
import boto3
import requests
from typing import Any, Dict
from .typing_inspection.arbeitnow import validate_api_response as validate_arbeitnow
from .typing_inspection.ba_api import validate_ba_response as validate_ba

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
    """Fetcher for the Bundesagentur für Arbeit (BA) API using OAuth2."""
    
    TOKEN_URL = "https://rest.arbeitsagentur.de/oauth/get-token"
    JOBS_URL = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/v1/jobsuche"

    def __init__(self, ssm_parameter_name: str = None):
        self.ssm_parameter_name = ssm_parameter_name or os.environ.get("SSM_PARAMETER_NAME")
        self.ssm = boto3.client("ssm")

    def _get_credentials(self) -> Dict[str, str]:
        """Retrieves OAuth2 credentials from SSM Parameter Store."""
        print(f"Retrieving credentials from SSM: {self.ssm_parameter_name}")
        response = self.ssm.get_parameter(
            Name=self.ssm_parameter_name, 
            WithDecryption=True
        )
        return json.loads(response["Parameter"]["Value"])

    def _get_access_token(self, client_id: str, client_secret: str) -> str:
        """Performs OAuth2 Client Credentials flow."""
        payload = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
        response = requests.post(self.TOKEN_URL, data=payload, timeout=10)
        response.raise_for_status()
        return response.json()["access_token"]

    def fetch_jobs(self, query: str = "Data Engineer") -> Dict[str, Any]:
        """Fetches all pages of jobs from BA API and validates them."""
        creds = self._get_credentials()
        token = self._get_access_token(creds["client_id"], creds["client_secret"])

        headers = {
            "Authorization": f"Bearer {token}",
            "X-API-Key": creds.get("api_key", ""),
            "Accept": "application/json"
        }

        all_jobs = []
        page = 1
        page_size = 100

        while True:
            params = {"was": query, "repro": "long", "page": page, "size": page_size}
            print(f"Fetching BA API page {page} for query: {query}")
            response = requests.get(self.JOBS_URL, headers=headers, params=params, timeout=15)
            response.raise_for_status()
            raw_data = response.json()
            validated = validate_ba(raw_data)
            jobs = validated.get('stellenangebote', [])
            if not jobs:
                break
            all_jobs.extend(jobs)
            max_results = validated.get('max_results', 0)
            if len(all_jobs) >= max_results:
                break
            page += 1

        print(f"Fetched {len(all_jobs)} total jobs from BA API.")
        return {'stellenangebote': all_jobs}

def get_fetcher(source: str) -> Any:
    """Factory function to get the appropriate fetcher."""
    if source == "arbeitnow":
        return ArbeitnowFetcher()
    elif source == "ba":
        return BAFetcher()
    else:
        raise ValueError(f"Unknown source: {source}")
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

    def fetch_jobs(self) -> Dict[str, Any]:
        print("Fetching data from Arbeitnow API...")
        response = requests.get(self.API_URL, timeout=10)
        response.raise_for_status()
        raw_data = response.json()
        
        # Trigger Pydantic validation immediately
        return validate_arbeitnow(raw_data)

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
        """Fetches jobs from BA API and validates them."""
        creds = self._get_credentials()
        token = self._get_access_token(creds["client_id"], creds["client_secret"])
        
        headers = {
            "Authorization": f"Bearer {token}",
            "X-API-Key": creds.get("api_key", ""),
            "Accept": "application/json"
        }
        
        params = {
            "was": query,
            "repro": "long"
        }

        print(f"Fetching data from BA API for query: {query}")
        response = requests.get(self.JOBS_URL, headers=headers, params=params, timeout=15)
        response.raise_for_status()
        raw_data = response.json()
        
        return validate_ba(raw_data)

def get_fetcher(source: str) -> Any:
    """Factory function to get the appropriate fetcher."""
    if source == "arbeitnow":
        return ArbeitnowFetcher()
    elif source == "ba":
        return BAFetcher()
    else:
        raise ValueError(f"Unknown source: {source}")
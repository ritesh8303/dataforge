import json
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import requests

from src.ingest_eures import (
    AntigravityClient,
    build_search_payload,
    extract_location,
    extract_tags,
    lambda_handler,
    normalize_eures_job,
)


class TestEuresIngestor(unittest.TestCase):
    def test_extract_location_map(self):
        item = {"locationMap": {"DE": ["DE7"], "FR": [None]}}
        self.assertEqual(extract_location(item), "DE (DE7); FR")

    def test_extract_location_legacy_list(self):
        item = {"locations": [{"cityName": "Berlin", "countryCode": "DE"}, {"cityName": "Paris", "countryCode": "FR"}]}
        self.assertEqual(extract_location(item), "Berlin, DE; Paris, FR")

    def test_extract_location_missing(self):
        self.assertEqual(extract_location({}), "")

    def test_extract_tags(self):
        item = {
            "positionScheduleCodes": ["fulltime"],
            "positionOfferingCode": "directhire",
            "jobCategoriesCodes": ["http://data.europa.eu/esco/occupation/example"],
        }
        self.assertEqual(extract_tags(item), "fulltime,directhire,example")

    def test_build_search_payload(self):
        payload = build_search_payload(["data engineer"], page=2, results_per_page=25, session_id="sess-1")
        self.assertEqual(payload["page"], 2)
        self.assertEqual(payload["resultsPerPage"], 25)
        self.assertEqual(payload["keywords"], [{"keyword": "data engineer", "specificSearchCode": "EVERYWHERE"}])
        self.assertEqual(payload["sessionId"], "sess-1")

    def test_normalize_eures_job(self):
        item = {
            "id": "vacancy_abc",
            "title": "Data Engineer",
            "employer": {"name": "Test Company"},
            "locationMap": {"DE": ["DE7"]},
            "description": "<b>Build pipelines</b>",
            "positionOfferingCode": "directhire",
            "positionScheduleCodes": ["fulltime"],
            "creationDate": 1739403609768,
        }
        job = normalize_eures_job(item)
        self.assertEqual(job["job_id"], "eures_vacancy_abc")
        self.assertEqual(job["source"], "eures")
        self.assertEqual(job["company"], "Test Company")
        self.assertEqual(job["location"], "DE (DE7)")
        self.assertEqual(job["description"], "Build pipelines")
        self.assertIn("directhire", job["job_types"])
        self.assertTrue(job["url"].endswith("/job?lang=en"))

    @patch("src.ingest_eures.requests.Session.post")
    def test_antigravity_client_retry(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.side_effect = [
            requests.exceptions.ConnectionError("Connection error"),
            requests.exceptions.Timeout("Timeout"),
            mock_response,
        ]

        client = AntigravityClient(retries=3, backoff_factor=0.1)
        res = client.post("http://dummy.url", json_payload={})
        self.assertEqual(res, mock_response)
        self.assertEqual(mock_post.call_count, 3)

    @patch("src.ingest_eures.antigravity.Client")
    @patch("processing.utils.save_parquet")
    @patch.dict("os.environ", {"BRONZE_BUCKET": "test-bucket", "LOCAL_RUN": "false"})
    def test_lambda_handler_success(self, mock_save, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "numberRecords": 1,
            "jvs": [
                {
                    "id": "vacancy_abc",
                    "title": "Data Engineer",
                    "employer": {"name": "Test Company"},
                    "locationMap": {"DE": ["DE7"]},
                    "description": "Awesome role",
                    "positionOfferingCode": "directhire",
                    "positionScheduleCodes": ["fulltime"],
                }
            ],
        }
        mock_client.post.return_value = mock_response

        res = lambda_handler({}, None)
        self.assertEqual(res["statusCode"], 200)
        self.assertIn("Successfully ingested", res["body"])

        mock_save.assert_called_once()
        df_passed = mock_save.call_args[0][0]
        self.assertIsInstance(df_passed, pd.DataFrame)
        self.assertEqual(len(df_passed), 1)
        self.assertEqual(df_passed.iloc[0]["job_id"], "eures_vacancy_abc")
        self.assertEqual(df_passed.iloc[0]["source"], "eures")


if __name__ == "__main__":
    unittest.main()

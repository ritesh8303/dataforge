import json
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd
import requests

from src.ingest_eures import (
    AntigravityClient,
    extract_location,
    extract_tags,
    lambda_handler,
    normalize_eures_job,
)


class TestEuresIngestor(unittest.TestCase):
    def test_extract_location_list(self):
        item = {"locations": [{"cityName": "Berlin", "countryCode": "DE"}, {"cityName": "Paris", "countryCode": "FR"}]}
        self.assertEqual(extract_location(item), "Berlin, DE; Paris, FR")

    def test_extract_location_dict(self):
        item = {"location": {"cityName": "Munich", "countryCode": "DE"}}
        self.assertEqual(extract_location(item), "Munich, DE")

    def test_extract_location_missing(self):
        self.assertEqual(extract_location({}), "")

    def test_extract_tags(self):
        item = {"categories": [{"name": "IT Services"}, {"name": "Software Development"}]}
        self.assertEqual(extract_tags(item), "IT Services,Software Development")

    def test_normalize_eures_job(self):
        item = {
            "id": "vacancy_abc",
            "title": "Data Engineer",
            "employer": {"name": "Test Company"},
            "locations": [{"cityName": "Hamburg", "countryCode": "DE"}],
            "url": "http://eures.url/jobs/vacancy_abc",
            "description": "Awesome role",
            "categories": [{"name": "Tech"}],
            "contractType": "permanent",
        }
        job = normalize_eures_job(item)
        self.assertEqual(job["job_id"], "eures_vacancy_abc")
        self.assertEqual(job["source"], "eures")
        self.assertEqual(job["job_types"], "permanent")
        self.assertEqual(job["tags"], "Tech")

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
            "totalNumberOfResults": 1,
            "results": [
                {
                    "id": "vacancy_abc",
                    "title": "Data Engineer",
                    "employer": {"name": "Test Company"},
                    "locations": [{"cityName": "Hamburg", "countryCode": "DE"}],
                    "url": "http://eures.url/jobs/vacancy_abc",
                    "description": "Awesome role",
                    "categories": [{"name": "Tech"}],
                    "contractType": "permanent",
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

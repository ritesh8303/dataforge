import unittest
from unittest.mock import patch, MagicMock
import json
import pandas as pd
from src.ingest_eures import extract_location, extract_tags, lambda_handler, AntigravityClient


class TestEuresIngestor(unittest.TestCase):
    def test_extract_location_list(self):
        item = {"locations": [{"cityName": "Berlin", "countryCode": "DE"}, {"cityName": "Paris", "countryCode": "FR"}]}
        loc = extract_location(item)
        self.assertEqual(loc, "Berlin, DE; Paris, FR")

    def test_extract_location_dict(self):
        item = {"location": {"cityName": "Munich", "countryCode": "DE"}}
        loc = extract_location(item)
        self.assertEqual(loc, "Munich, DE")

    def test_extract_location_missing(self):
        item = {}
        loc = extract_location(item)
        self.assertEqual(loc, "Unknown Location")

    def test_extract_tags(self):
        item = {"categories": [{"name": "IT Services"}, {"name": "Software Development"}]}
        tags = extract_tags(item)
        self.assertEqual(json.loads(tags), ["IT Services", "Software Development"])

    @patch("src.ingest_eures.requests.Session.post")
    def test_antigravity_client_retry(self, mock_post):
        # Setup mock to fail twice and succeed once
        mock_response = MagicMock()
        mock_response.status_code = 200
        import requests

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
    @patch("src.ingest_eures.save_parquet")
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

        event = {}
        context = None

        res = lambda_handler(event, context)
        self.assertEqual(res["statusCode"], 200)
        self.assertIn("Successfully ingested", res["body"])

        # Verify save_parquet was called
        mock_save.assert_called_once()
        df_passed = mock_save.call_args[0][0]
        self.assertIsInstance(df_passed, pd.DataFrame)
        self.assertEqual(len(df_passed), 1)
        self.assertEqual(df_passed.iloc[0]["job_id"], "eures_vacancy_abc")
        self.assertEqual(df_passed.iloc[0]["title"], "Data Engineer")
        self.assertEqual(df_passed.iloc[0]["company"], "Test Company")
        self.assertEqual(df_passed.iloc[0]["location"], "Hamburg, DE")
        self.assertEqual(df_passed.iloc[0]["source"], "EURES")


if __name__ == "__main__":
    unittest.main()

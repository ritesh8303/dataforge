import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from src import ingest_eures
from src.ingest_eures import (
    AntigravityClient,
    _job_url,
    build_search_payload,
    extract_location,
    extract_tags,
    lambda_handler,
    normalize_eures_job,
)

eures_requests = ingest_eures.requests


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

    def test_job_url_encodes_spaces(self):
        url = _job_url("MTQ3NDk0MzMgMTI", "pl")
        self.assertIn("/jv-details/MTQ3NDk0MzMgMTI?", url)
        self.assertIn("jvDisplayLanguage=pl", url)

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
        self.assertEqual(job["job_id"], "eures_vacancy_abc")  # alphanumeric ids unchanged
        self.assertEqual(job["source"], "eures")
        self.assertEqual(job["company"], "Test Company")
        self.assertEqual(job["location"], "DE (DE7)")
        self.assertEqual(job["description"], "Build pipelines")
        self.assertIn("directhire", job["job_types"])
        self.assertIn("/jv-details/vacancy_abc?", job["url"])
        self.assertIn("jvDisplayLanguage=en", job["url"])

    @patch("src.ingest_eures.requests.Session.post")
    def test_antigravity_client_retry(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.side_effect = [
            eures_requests.exceptions.ConnectionError("Connection error"),
            eures_requests.exceptions.Timeout("Timeout"),
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


    def test_import_site_package_avoids_src_shadow(self):
        from src.processing.site_imports import import_site_package

        src_dir = Path(__file__).resolve().parents[1] / "src"
        shadow = src_dir / "requests"
        if not shadow.is_dir():
            self.skipTest("src/requests vendor shadow not present in this environment")

        mod = import_site_package("requests")
        self.assertTrue(hasattr(mod, "Session"))
        self.assertNotEqual(getattr(mod, "__file__", ""), str(shadow / "__init__.py"))


if __name__ == "__main__":
    unittest.main()

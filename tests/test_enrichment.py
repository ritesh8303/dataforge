import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from enrichment.enricher import JobEnricher, enrich_jobs_dataframe
import pandas as pd


def test_enrich_job_returns_schema():
    enricher = JobEnricher()
    row = {
        "job_id": "test-1",
        "title": "Data Engineer Python AWS",
        "company": "TestCo",
        "location": "Berlin",
        "description": "Build pipelines with Python and AWS Spark.",
    }
    result = enricher.enrich_job(row)
    assert result["job_id"] == "test-1"
    assert "ai_skills" in result
    skills = json.loads(result["ai_skills"])
    assert isinstance(skills, list)


def test_enrich_jobs_dataframe_empty():
    df = enrich_jobs_dataframe(pd.DataFrame())
    assert df.empty

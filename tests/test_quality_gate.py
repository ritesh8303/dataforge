from pathlib import Path

import pandas as pd

from processing.data_quality import evaluate_gold_quality_row

GOLD_QUALITY = Path(__file__).resolve().parents[1] / "data" / "gold" / "data_quality_report.csv"


def test_committed_gold_quality_passes_gate():
    df = pd.read_csv(GOLD_QUALITY)
    row = df.iloc[-1].to_dict()
    assert evaluate_gold_quality_row(row) == []


def test_gate_fails_on_invalid_source():
    row = {
        "schema_validation_pass": True,
        "missing_title_rate": 0.0,
        "missing_company_rate": 0.0,
        "duplicate_job_id_rate": 0.0,
        "invalid_source_count": 3,
        "remote_in_country_dimension": 0,
    }
    assert "invalid_source_count=3" in evaluate_gold_quality_row(row)

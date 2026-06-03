import pandas as pd
from src.silver_transformer import process_scd_type_2
from unittest.mock import patch

# awswrangler is not installed in CI — stub the exception class it provides
try:
    import awswrangler as wr

    _NoFilesFound = wr.exceptions.NoFilesFound
except ImportError:
    _NoFilesFound = type("NoFilesFound", (Exception,), {})


def _make_bronze_row(job_id, title, company="DataForge", location="Berlin", source="arbeitnow"):
    return {
        "job_id": job_id,
        "title": title,
        "company": company,
        "location": location,
        "source": source,
        "ingested_at": "2025-01-15T08:00:00+00:00",
    }


def test_scd_first_run():
    """First run with empty Silver — all records inserted as current."""
    bronze_data = pd.DataFrame(
        [
            _make_bronze_row("job_001", "Data Engineer"),
            _make_bronze_row("job_002", "Senior Data Engineer"),
        ]
    )

    with (
        patch("src.silver_transformer.wr.s3.read_parquet", side_effect=_NoFilesFound),
        patch("src.silver_transformer.wr.s3.to_parquet") as mock_write,
    ):
        process_scd_type_2(bronze_data, "s3://dummy/silver.parquet")
        result_df = mock_write.call_args[1]["df"]

        assert len(result_df) == 2
        assert result_df["is_current"].all()
        assert set(result_df["job_id"]) == {
            "sem_dataforge_data-engineer_berlin",
            "sem_dataforge_senior-data-engineer_berlin",
        }


def test_scd_unchanged_record():
    """Second run with same data — no changes, Silver untouched."""
    bronze_data = pd.DataFrame([_make_bronze_row("job_001", "Data Engineer")])

    existing_silver = bronze_data.copy()
    existing_silver["hash_key"] = "abc123"
    existing_silver["scd_start_date"] = pd.Timestamp("2025-01-14", tz="UTC")
    existing_silver["scd_end_date"] = pd.NaT
    existing_silver["is_current"] = True

    with (
        patch("src.silver_transformer.wr.s3.read_parquet", return_value=existing_silver),
        patch("src.silver_transformer.wr.s3.to_parquet") as mock_write,
    ):
        process_scd_type_2(bronze_data, "s3://dummy/silver.parquet")
        result_df = mock_write.call_args[1]["df"]

        current = result_df[result_df["is_current"]]
        assert len(current) == 1
        assert current.iloc[0]["job_id"] == "sem_dataforge_data-engineer_berlin"


def test_scd_updated_record():
    """Job source changes — old record expired, new version inserted under same semantic key."""
    old_bronze = pd.DataFrame([_make_bronze_row("job_001", "Data Engineer", source="arbeitnow")])
    new_bronze = pd.DataFrame([_make_bronze_row("job_001", "Data Engineer", source="ba_api")])

    with (
        patch("src.silver_transformer.wr.s3.read_parquet", side_effect=_NoFilesFound),
        patch("src.silver_transformer.wr.s3.to_parquet") as mock_write,
    ):
        process_scd_type_2(old_bronze, "s3://dummy/silver.parquet")
        existing_silver = mock_write.call_args[1]["df"]

    with (
        patch("src.silver_transformer.wr.s3.read_parquet", return_value=existing_silver),
        patch("src.silver_transformer.wr.s3.to_parquet") as mock_write,
    ):
        process_scd_type_2(new_bronze, "s3://dummy/silver.parquet")
        result_df = mock_write.call_args[1]["df"]

        assert len(result_df) == 2
        expired = result_df[~result_df["is_current"]]
        current = result_df[result_df["is_current"]]
        assert len(expired) == 1
        assert len(current) == 1
        assert expired.iloc[0]["source"] == "arbeitnow"
        assert current.iloc[0]["source"] == "ba_api"
        assert pd.notna(expired.iloc[0]["scd_end_date"])


def test_scd_new_job_added():
    """New job_id appears in Bronze — inserted alongside existing records."""
    existing_row = _make_bronze_row("job_001", "Data Engineer")
    new_row = _make_bronze_row("job_002", "ML Engineer")

    existing_silver = pd.DataFrame([existing_row])
    existing_silver["hash_key"] = "abc123"
    existing_silver["scd_start_date"] = pd.Timestamp("2025-01-14", tz="UTC")
    existing_silver["scd_end_date"] = pd.NaT
    existing_silver["is_current"] = True

    with (
        patch("src.silver_transformer.wr.s3.read_parquet", return_value=existing_silver),
        patch("src.silver_transformer.wr.s3.to_parquet") as mock_write,
    ):
        process_scd_type_2(pd.DataFrame([existing_row, new_row]), "s3://dummy/silver.parquet")
        result_df = mock_write.call_args[1]["df"]

        current = result_df[result_df["is_current"]]
        assert len(current) == 2
        assert set(current["job_id"]) == {"sem_dataforge_data-engineer_berlin", "sem_dataforge_ml-engineer_berlin"}


def test_scd_missing_job_id_raises():
    """Bronze data without job_id column raises a clear ValueError."""
    bad_bronze = pd.DataFrame([{"title": "Data Engineer", "company": "Test"}])

    with patch("src.silver_transformer.wr.s3.read_parquet", side_effect=_NoFilesFound):
        try:
            process_scd_type_2(bad_bronze, "s3://dummy/silver.parquet")
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "job_id" in str(e)


def test_scd_s3_read_error_raises():
    """A real S3 error raises RuntimeError — not silently treated as first run."""
    bronze_data = pd.DataFrame([_make_bronze_row("job_001", "Data Engineer")])

    with patch("src.silver_transformer.wr.s3.read_parquet", side_effect=Exception("S3 AccessDenied")):
        try:
            process_scd_type_2(bronze_data, "s3://dummy/silver.parquet")
            assert False, "Should have raised RuntimeError"
        except RuntimeError as e:
            assert "Failed to read Silver layer" in str(e)

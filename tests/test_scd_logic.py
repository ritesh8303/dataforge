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
        patch("src.silver_transformer.wr.s3.list_objects", return_value=[]),
        patch("src.silver_transformer.wr.s3.to_parquet") as mock_write,
    ):
        process_scd_type_2(bronze_data, "s3://dummy/silver.parquet")
        
        dfs = []
        for call in mock_write.call_args_list:
            df = call[1].get("df") if "df" in call[1] else call[0][0]
            path = call[1].get("path") if "path" in call[1] else call[0][1]
            df = df.copy()
            if "is_current=True" in path:
                df["is_current"] = True
            elif "is_current=False" in path:
                df["is_current"] = False
            dfs.append(df)
        result_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

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
        patch("src.silver_transformer.wr.s3.list_objects", side_effect=lambda path: ["file"] if "is_current=True" in path else []),
        patch("src.silver_transformer.wr.s3.to_parquet") as mock_write,
    ):
        process_scd_type_2(bronze_data, "s3://dummy/silver.parquet")
        
        dfs = []
        for call in mock_write.call_args_list:
            df = call[1].get("df") if "df" in call[1] else call[0][0]
            path = call[1].get("path") if "path" in call[1] else call[0][1]
            df = df.copy()
            if "is_current=True" in path:
                df["is_current"] = True
            elif "is_current=False" in path:
                df["is_current"] = False
            dfs.append(df)
        result_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        current = result_df[result_df["is_current"]]
        assert len(current) == 1
        assert current.iloc[0]["job_id"] == "sem_dataforge_data-engineer_berlin"


def test_scd_updated_record():
    """Job source changes — old record expired, new version inserted under same semantic key."""
    old_bronze = pd.DataFrame([_make_bronze_row("job_001", "Data Engineer", source="arbeitnow")])
    new_bronze = pd.DataFrame([_make_bronze_row("job_001", "Data Engineer", source="ba_api")])

    with (
        patch("src.silver_transformer.wr.s3.read_parquet", side_effect=_NoFilesFound),
        patch("src.silver_transformer.wr.s3.list_objects", return_value=[]),
        patch("src.silver_transformer.wr.s3.to_parquet") as mock_write,
    ):
        process_scd_type_2(old_bronze, "s3://dummy/silver.parquet")
        
        dfs = []
        for call in mock_write.call_args_list:
            df = call[1].get("df") if "df" in call[1] else call[0][0]
            path = call[1].get("path") if "path" in call[1] else call[0][1]
            df = df.copy()
            if "is_current=True" in path:
                df["is_current"] = True
            elif "is_current=False" in path:
                df["is_current"] = False
            dfs.append(df)
        existing_silver = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    with (
        patch("src.silver_transformer.wr.s3.read_parquet", return_value=existing_silver),
        patch("src.silver_transformer.wr.s3.list_objects", side_effect=lambda path: ["file"] if "is_current=True" in path else []),
        patch("src.silver_transformer.wr.s3.to_parquet") as mock_write,
    ):
        process_scd_type_2(new_bronze, "s3://dummy/silver.parquet")
        
        dfs = []
        for call in mock_write.call_args_list:
            df = call[1].get("df") if "df" in call[1] else call[0][0]
            path = call[1].get("path") if "path" in call[1] else call[0][1]
            df = df.copy()
            if "is_current=True" in path:
                df["is_current"] = True
            elif "is_current=False" in path:
                df["is_current"] = False
            dfs.append(df)
        result_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

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
        patch("src.silver_transformer.wr.s3.list_objects", side_effect=lambda path: ["file"] if "is_current=True" in path else []),
        patch("src.silver_transformer.wr.s3.to_parquet") as mock_write,
    ):
        process_scd_type_2(pd.DataFrame([existing_row, new_row]), "s3://dummy/silver.parquet")
        
        dfs = []
        for call in mock_write.call_args_list:
            df = call[1].get("df") if "df" in call[1] else call[0][0]
            path = call[1].get("path") if "path" in call[1] else call[0][1]
            df = df.copy()
            if "is_current=True" in path:
                df["is_current"] = True
            elif "is_current=False" in path:
                df["is_current"] = False
            dfs.append(df)
        result_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

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


def test_scd_silver_read_anomaly_blocks_empty_active():
    """Active partition files that read as empty must abort."""
    bronze_data = pd.DataFrame([_make_bronze_row("job_001", "Data Engineer")])
    active_key = "s3://dummy/silver.parquet/is_current=True/part.parquet"
    inactive_key = "s3://dummy/silver.parquet/is_current=False/part.parquet"

    def _read_parquet(path):
        if "is_current=True" in path:
            return pd.DataFrame()
        inactive = bronze_data.copy()
        inactive["hash_key"] = "abc123"
        inactive["scd_start_date"] = pd.Timestamp("2025-01-14", tz="UTC")
        inactive["scd_end_date"] = pd.Timestamp("2025-01-15", tz="UTC")
        inactive["is_current"] = False
        return inactive

    with (
        patch("src.silver_transformer.wr.s3.read_parquet", side_effect=_read_parquet),
        patch(
            "src.silver_transformer.wr.s3.list_objects",
            side_effect=lambda path: [active_key] if "is_current=True" in path else [inactive_key],
        ),
        patch("src.silver_transformer.wr.s3.to_parquet"),
    ):
        try:
            process_scd_type_2(bronze_data, "s3://dummy/silver.parquet/")
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "SILVER_READ_ANOMALY" in str(e)


def test_bronze_vs_silver_micro_batch_guard():
    from src.silver_transformer import _validate_bronze_vs_silver

    try:
        _validate_bronze_vs_silver(50, 20000)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "DATA_QUALITY_ANOMALY" in str(e)

    # Daily Bronze is smaller than cumulative Silver — must not abort.
    _validate_bronze_vs_silver(9976, 21064)

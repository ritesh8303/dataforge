import pandas as pd
import pytest
from src.silver_transformer import process_scd_type_2
from unittest.mock import patch

# awswrangler is not installed in CI — stub the exception class it provides
try:
    import awswrangler as wr

    _NoFilesFound = wr.exceptions.NoFilesFound
except ImportError:
    _NoFilesFound = type("NoFilesFound", (Exception,), {})


@pytest.fixture(autouse=True)
def _noop_silver_s3_maintenance():
    """Silver purge/clear use boto3 directly; keep unit tests offline."""
    with (
        patch("src.silver_transformer._purge_inactive_silver", return_value=0),
        patch("src.silver_transformer._clear_inactive_silver", return_value=0),
    ):
        yield

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
            else:
                continue  # skip non-partition writes (gold trigger marker, stats)
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
            else:
                continue  # skip non-partition writes (gold trigger marker, stats)
            dfs.append(df)
        result_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        current = result_df[result_df["is_current"]]
        assert len(current) == 1
        assert current.iloc[0]["job_id"] == "sem_dataforge_data-engineer_berlin"


def test_scd_updated_record():
    """Job attributes change (job_types) — old record expired, new version inserted."""
    old_bronze = pd.DataFrame(
        [{**_make_bronze_row("job_001", "Data Engineer"), "job_types": "full-time"}]
    )
    new_bronze = pd.DataFrame(
        [{**_make_bronze_row("job_001", "Data Engineer"), "job_types": "part-time"}]
    )

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
            else:
                continue  # skip non-partition writes (gold trigger marker, stats)
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
            else:
                continue  # skip non-partition writes (gold trigger marker, stats)
            dfs.append(df)
        result_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        assert len(result_df) == 2
        expired = result_df[~result_df["is_current"]]
        current = result_df[result_df["is_current"]]
        assert len(expired) == 1
        assert len(current) == 1
        assert expired.iloc[0]["job_types"] == "full-time"
        assert current.iloc[0]["job_types"] == "part-time"
        assert pd.notna(expired.iloc[0]["scd_end_date"])


def test_scd_source_flip_is_not_an_update():
    """The same semantic job re-appearing under a different source must not churn history."""
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
            else:
                continue  # skip non-partition writes (gold trigger marker, stats)
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
            else:
                continue  # skip non-partition writes (gold trigger marker, stats)
            dfs.append(df)
        result_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

        # No expired version, no new version — the original record survives untouched.
        assert len(result_df) == 1
        assert result_df["is_current"].all()
        assert result_df.iloc[0]["source"] == "arbeitnow"


def test_scd_source_pull_grace_period():
    """A job missing from today's pull but seen recently stays active (grace window)."""
    existing_row = _make_bronze_row("job_001", "Data Engineer", source="arbeitnow")
    existing_silver = pd.DataFrame([existing_row])
    existing_silver["hash_key"] = "abc123"
    existing_silver["scd_start_date"] = pd.Timestamp("2025-01-14", tz="UTC")
    existing_silver["scd_end_date"] = pd.NaT
    existing_silver["is_current"] = True
    existing_silver["job_id"] = "sem_dataforge_data-engineer_berlin"
    # Seen yesterday — within the 2-day grace window.
    existing_silver["last_seen_at"] = (
        pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)
    ).isoformat()

    empty_bronze = pd.DataFrame(
        columns=["job_id", "title", "company", "location", "source", "url", "ingested_at"]
    )

    with (
        patch("src.silver_transformer.wr.s3.read_parquet", return_value=existing_silver),
        patch(
            "src.silver_transformer.wr.s3.list_objects",
            side_effect=lambda path: ["file"] if "is_current=True" in path else [],
        ),
        patch("src.silver_transformer.wr.s3.to_parquet") as mock_write,
    ):
        process_scd_type_2(
            empty_bronze,
            "s3://dummy/silver.parquet/",
            source_pull_index={"arbeitnow": set()},
            sources_in_pull={"arbeitnow"},
        )

        active_writes = [
            call for call in mock_write.call_args_list if "is_current=True" in str(call)
        ]
        assert active_writes, "Expected active partition write"
        active_df = active_writes[-1][1].get("df") if "df" in active_writes[-1][1] else active_writes[-1][0][0]
        assert len(active_df) == 1, "Job within grace window must stay active"


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
            else:
                continue  # skip non-partition writes (gold trigger marker, stats)
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
    """Unreadable active Silver files must abort — not silently treated as first run."""
    bronze_data = pd.DataFrame([_make_bronze_row("job_001", "Data Engineer")])
    active_key = "s3://dummy/silver.parquet/is_current=True/part.parquet"

    with (
        patch("src.silver_transformer.wr.s3.read_parquet", side_effect=Exception("S3 AccessDenied")),
        patch(
            "src.silver_transformer.wr.s3.list_objects",
            side_effect=lambda path: [active_key] if "is_current=True" in path else [],
        ),
        patch("src.silver_transformer.wr.s3.to_parquet"),
    ):
        try:
            process_scd_type_2(bronze_data, "s3://dummy/silver.parquet")
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "SILVER_READ_ANOMALY" in str(e)


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


def test_scd_source_pull_expiration():
    """Active jobs missing from their source's daily Bronze pull are expired."""
    existing_row = _make_bronze_row("job_001", "Data Engineer", source="arbeitnow")
    existing_silver = pd.DataFrame([existing_row])
    existing_silver["hash_key"] = "abc123"
    existing_silver["scd_start_date"] = pd.Timestamp("2025-01-14", tz="UTC")
    existing_silver["scd_end_date"] = pd.NaT
    existing_silver["is_current"] = True
    existing_silver["job_id"] = "sem_dataforge_data-engineer_berlin"
    # Last confirmed by its source well beyond the grace window.
    existing_silver["last_seen_at"] = (
        pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=10)
    ).isoformat()

    # Today's pull has arbeitnow ingested but this job is gone from the source feed.
    empty_bronze = pd.DataFrame(
        columns=["job_id", "title", "company", "location", "source", "url", "ingested_at"]
    )
    source_pull_index = {"arbeitnow": set()}
    sources_in_pull = {"arbeitnow"}

    with (
        patch("src.silver_transformer.wr.s3.read_parquet", return_value=existing_silver),
        patch(
            "src.silver_transformer.wr.s3.list_objects",
            side_effect=lambda path: ["file"] if "is_current=True" in path else [],
        ),
        patch("src.silver_transformer.wr.s3.to_parquet") as mock_write,
    ):
        process_scd_type_2(
            empty_bronze,
            "s3://dummy/silver.parquet/",
            source_pull_index=source_pull_index,
            sources_in_pull=sources_in_pull,
        )

        active_writes = [
            call for call in mock_write.call_args_list if "is_current=True" in str(call)
        ]
        inactive_writes = [
            call for call in mock_write.call_args_list if "is_current=False" in str(call)
        ]
        assert active_writes, "Expected active partition write"
        assert inactive_writes, "Expected inactive partition write for expired job"
        active_df = active_writes[-1][1].get("df") if "df" in active_writes[-1][1] else active_writes[-1][0][0]
        assert len(active_df) == 0

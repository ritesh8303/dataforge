import json
import os
import shutil
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def _flatten_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce all columns to Parquet-safe scalars (PyArrow-compatible)."""
    flat = df.copy()
    for col in flat.columns:
        if flat[col].dtype == object:
            flat[col] = flat[col].apply(
                lambda x: (
                    json.dumps(x, ensure_ascii=False)
                    if isinstance(x, (dict, list))
                    else ("" if x is None or (isinstance(x, float) and pd.isna(x)) else str(x))
                )
            )
    return flat


def _parse_s3_uri(path: str) -> tuple[str, str]:
    parsed = urlparse(path)
    return parsed.netloc, parsed.path.lstrip("/")


def _write_parquet_table(table: pa.Table, path: str) -> str:
    """Write a PyArrow table to a local path or s3:// URI (single file)."""
    if path.startswith("s3://"):
        bucket, key = _parse_s3_uri(path)
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer, compression="snappy", use_dictionary=False)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buffer.getvalue().to_pybytes())
        return path

    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    if os.path.isdir(path):
        shutil.rmtree(path)
    elif os.path.isfile(path):
        os.remove(path)
    pq.write_table(table, path, compression="snappy", use_dictionary=False)
    return path


def save_parquet(df, path, source):
    """
    Saves a DataFrame as Parquet (Snappy, single file, PyArrow-compatible).
    If LOCAL_RUN=true, writes to data/bronze/; otherwise uses awswrangler S3.
    """
    flat_df = _flatten_for_parquet(df)

    table = pa.Table.from_pandas(flat_df, preserve_index=False)

    if os.environ.get("LOCAL_RUN") == "true":
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        local_dir = os.path.join("data", "bronze", source, f"ingested_at={date_str}")
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, "jobs.parquet")
        written = _write_parquet_table(table, local_path)
        print(f"Successfully saved {len(flat_df)} jobs locally to: {written}")
        return written

    written = _write_parquet_table(table, path)
    print(f"Successfully saved {len(flat_df)} jobs to S3: {written}")
    return written

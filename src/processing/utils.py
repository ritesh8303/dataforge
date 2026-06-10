import json
import os
import shutil
from datetime import datetime, timezone

import awswrangler as wr
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
        if os.path.isdir(local_path):
            shutil.rmtree(local_path)
        elif os.path.isfile(local_path):
            os.remove(local_path)
        pq.write_table(table, local_path, compression="snappy", use_dictionary=False)
        print(f"Successfully saved {len(flat_df)} jobs locally to: {local_path}")
        return local_path

    wr.s3.to_parquet(
        df=flat_df,
        path=path,
        index=False,
        compression="snappy",
        dataset=False,
        pyarrow_additional_kwargs={"use_dictionary": False},
    )
    print(f"Successfully saved {len(flat_df)} jobs to S3: {path}")
    return path

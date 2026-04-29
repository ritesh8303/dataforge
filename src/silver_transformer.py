import hashlib
import pandas as pd
import awswrangler as wr
from datetime import datetime, timezone
import os


def lambda_handler(event, context):
    """
    AWS Lambda entry point triggered by S3 ObjectCreated events.
    Reads a new Bronze Parquet file and applies SCD Type 2 logic to Silver.
    """
    silver_path = os.environ.get('SILVER_PATH')

    if not silver_path:
        raise ValueError("SILVER_PATH environment variable is not set.")

    for record in event.get('Records', []):
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        print(f"Detected new data in Bronze: s3://{bucket}/{key}")

        bronze_df = wr.s3.read_parquet(path=f"s3://{bucket}/{key}")
        process_scd_type_2(bronze_df, silver_path)


def generate_hash(df, cols):
    """Creates a SHA256 hash of specific columns to detect attribute changes."""
    # Only hash columns that actually exist in the dataframe
    existing_cols = [c for c in cols if c in df.columns]
    return df[existing_cols].astype(str).apply(
        lambda x: hashlib.sha256("".join(x).encode()).hexdigest(), axis=1
    )


def process_scd_type_2(bronze_df, silver_path):
    """
    Implements SCD Type 2 logic:
    1. Identifies new records.
    2. Identifies changed records (expires old, inserts new version).
    3. Persists results to the Silver Layer.
    """
    now = datetime.now(timezone.utc)

    # Columns used to detect changes — all present after Bronze normalization
    attr_cols = ['title', 'company', 'location', 'source']

    # Validate that job_id exists — both ingestors now produce this column
    if 'job_id' not in bronze_df.columns:
        raise ValueError(
            "Bronze data is missing 'job_id' column. "
            "Ensure ingestors rename slug/refnr to job_id before writing."
        )

    # 1. Prepare incoming Bronze data
    bronze_df = bronze_df.copy()
    bronze_df['hash_key'] = generate_hash(bronze_df, attr_cols)
    bronze_df['scd_start_date'] = now
    bronze_df['scd_end_date'] = pd.NaT
    bronze_df['is_current'] = True

    # 2. Load existing Silver data
    silver_exists = False
    silver_df = pd.DataFrame()

    try:
        silver_df = wr.s3.read_parquet(path=silver_path)
        silver_exists = True
        print(f"Loaded {len(silver_df)} existing Silver records.")
    except wr.exceptions.NoFilesFound:
        # Expected on first run — Silver bucket is empty
        print("Silver layer is empty. Performing initial load.")
    except Exception as e:
        # Re-raise anything that is NOT a missing file error
        # so we don't silently overwrite Silver with bad data
        raise RuntimeError(f"Failed to read Silver layer: {str(e)}") from e

    # First run — write everything directly
    if not silver_exists or silver_df.empty:
        wr.s3.to_parquet(
            df=bronze_df,
            path=silver_path,
            dataset=True,
            mode="overwrite"
        )
        print(f"Initial load complete. Wrote {len(bronze_df)} records to Silver.")
        return

    # 3. Separate current from historical Silver records
    current_silver = silver_df[silver_df['is_current']].copy()
    historical_records = silver_df[~silver_df['is_current']].copy()

    # 4. Detect changes by merging on job_id
    merged = pd.merge(
        current_silver[['job_id', 'hash_key']],
        bronze_df[['job_id', 'hash_key']],
        on='job_id',
        how='outer',
        suffixes=('_old', '_new')
    )

    # Case A — job exists in Silver but hash changed → expire old record
    changed_mask = (
        merged['hash_key_old'].notna() &
        merged['hash_key_new'].notna() &
        (merged['hash_key_old'] != merged['hash_key_new'])
    )
    changed_ids = merged.loc[changed_mask, 'job_id'].tolist()

    expired_records = current_silver[
        current_silver['job_id'].isin(changed_ids)
    ].copy()
    expired_records['is_current'] = False
    expired_records['scd_end_date'] = now

    # Case B — new job_id not in Silver at all → insert
    new_mask = merged['hash_key_old'].isna()
    new_ids = merged.loc[new_mask, 'job_id'].tolist()

    # New inserts = brand new jobs + updated versions of changed jobs
    insert_ids = set(new_ids + changed_ids)
    new_inserts = bronze_df[bronze_df['job_id'].isin(insert_ids)].copy()

    # 5. Unchanged current records — keep as-is
    unchanged_silver = current_silver[
        ~current_silver['job_id'].isin(changed_ids)
    ].copy()

    # 6. Build final dataset
    final_df = pd.concat(
        [historical_records, unchanged_silver, expired_records, new_inserts],
        ignore_index=True
    )

    # 7. Deduplication safety net — remove any accidental duplicates
    final_df = final_df.sort_values(
        by=['job_id', 'scd_start_date'], ascending=[True, False]
    )
    final_df = final_df.drop_duplicates(
        subset=['job_id', 'is_current'], keep='first'
    )

    # 8. Write back to Silver S3 (no Glue catalog dependency)
    wr.s3.to_parquet(
        df=final_df,
        path=silver_path,
        dataset=True,
        mode="overwrite"
    )

    print(
        f"SCD Type 2 complete. "
        f"New: {len(new_ids)}, Updated: {len(changed_ids)}, "
        f"Unchanged: {len(unchanged_silver)}, "
        f"Total Silver records: {len(final_df)}"
    )

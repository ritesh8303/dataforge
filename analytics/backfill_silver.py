"""
One-time backfill script: reads all Bronze JSON files from S3,
normalizes them to the unified schema, and runs SCD Type 2 into Silver.
"""
import sys
import json
import boto3
import pandas as pd
import awswrangler as wr
from datetime import datetime, timezone

sys.path.insert(0, 'src')
from silver_transformer import process_scd_type_2

BRONZE_BUCKET = "dataforge-bronze-dev-eu-central-1"
SILVER_PATH   = "s3://dataforge-silver-dev-eu-central-1/cleaned/jobs_history.parquet/"
REGION        = "eu-central-1"

s3 = boto3.client("s3", region_name=REGION)

def list_bronze_json_files():
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix="ingested/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return keys

def normalize_arbeitnow(jobs):
    df = pd.DataFrame(jobs)
    for col in ['tags', 'job_types']:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: ','.join(x) if isinstance(x, list) else str(x))
    df.rename(columns={'slug': 'job_id', 'company_name': 'company'}, inplace=True)
    df['source'] = 'arbeitnow'
    df['ingested_at'] = datetime.now(timezone.utc).isoformat()
    return df

def normalize_ba(jobs):
    df = pd.DataFrame(jobs)
    if 'arbeitsort' in df.columns:
        loc = df['arbeitsort'].apply(lambda x: x if isinstance(x, dict) else {}).apply(pd.Series)
        df['location'] = loc.get('ort', pd.Series([''] * len(df)))
    df.rename(columns={
        'refnr': 'job_id', 'titel': 'title', 'arbeitgeber': 'company',
        'eintrittsdatum': 'start_date_raw', 'modifikationsTimestamp': 'modified_at'
    }, inplace=True)
    df['source'] = 'ba_api'
    df['ingested_at'] = datetime.now(timezone.utc).isoformat()
    return df

def process_file(key):
    obj = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
    content = obj['Body'].read().decode('utf-8').strip()

    # Handle both single JSON and newline-delimited JSON (NDJSON)
    try:
        raw = json.loads(content)
        raw_list = [raw]
    except json.JSONDecodeError:
        raw_list = [json.loads(line) for line in content.splitlines() if line.strip()]

    total = 0
    for raw in raw_list:
        if 'data' in raw and raw['data']:
            df = normalize_arbeitnow(raw['data'])
        elif 'stellenangebote' in raw and raw['stellenangebote']:
            df = normalize_ba(raw['stellenangebote'])
        else:
            continue

        if 'job_id' not in df.columns:
            continue

        df = df[df['job_id'].notna() & (df['job_id'] != '')]
        process_scd_type_2(df, SILVER_PATH)
        total += len(df)

    if total == 0:
        print(f"  Skipping {key} — unrecognised or empty")
    return total

if __name__ == "__main__":
    keys = list_bronze_json_files()
    print(f"Found {len(keys)} Bronze JSON files to backfill.")

    total = 0
    for i, key in enumerate(keys, 1):
        print(f"[{i}/{len(keys)}] Processing {key}...")
        try:
            n = process_file(key)
            total += n
            print(f"  -> {n} records processed.")
        except Exception as e:
            print(f"  ERROR: {e}")

    print(f"\nBackfill complete. Total records processed: {total}")
    print("Re-run analytics/query_gold.py to generate updated gold outputs.")

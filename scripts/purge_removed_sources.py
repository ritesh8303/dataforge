"""One-off: purge indeed/hacker_news rows from Silver and Bronze S3 prefixes."""
from __future__ import annotations

import os
import sys

import awswrangler as wr
import boto3
import pandas as pd

REMOVED = {"indeed", "hacker_news"}
BRONZE_PREFIXES = ["apify_indeed/", "hacker_news/"]
REGION = "eu-central-1"
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "dataforge-bronze-dev-eu-central-1")
SILVER_PATH = os.environ.get(
    "SILVER_PATH",
    "s3://dataforge-silver-dev-eu-central-1/cleaned/jobs_history.parquet/",
)


def purge_bronze(s3):
    for prefix in BRONZE_PREFIXES:
        print(f"Deleting s3://{BRONZE_BUCKET}/{prefix}...")
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=prefix):
            keys = [o["Key"] for o in page.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=BRONZE_BUCKET, Delete={"Objects": [{"Key": k} for k in keys]})
                print(f"  Deleted {len(keys)} objects under {prefix}")


def purge_silver_partition(path: str) -> int:
    removed = 0
    try:
        files = wr.s3.list_objects(path=path)
    except Exception:
        return 0
    for f in files:
        if not f:
            continue
        df = wr.s3.read_parquet(path=f)
        before = len(df)
        if "source" not in df.columns:
            continue
        df = df[~df["source"].isin(REMOVED)].copy()
        removed += before - len(df)
        if len(df) == 0:
            continue
        wr.s3.to_parquet(df=df, path=f, index=False)
    return removed


def main():
    s3 = boto3.client("s3", region_name=REGION)
    purge_bronze(s3)
    active_removed = purge_silver_partition(f"{SILVER_PATH}is_current=True/")
    inactive_removed = purge_silver_partition(f"{SILVER_PATH}is_current=False/")
    print(f"Silver purge complete. Removed {active_removed + inactive_removed} rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

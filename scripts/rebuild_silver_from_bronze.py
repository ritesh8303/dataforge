"""
Rebuild active Silver from Bronze history after a bad transformer overwrite.

Merges the last N days of Bronze partitions (latest row per job_id), then
force-writes the active Silver partition. Historical expired rows are kept.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from silver_transformer import load_bronze_history, process_scd_type_2  # noqa: E402

REGION = "eu-central-1"
BRONZE_BUCKET = "dataforge-bronze-dev-eu-central-1"
SILVER_PATH = "s3://dataforge-silver-dev-eu-central-1/cleaned/jobs_history.parquet/"
GOLD_BUCKET = "dataforge-gold-dev-eu-central-1"


def invoke_gold_generator() -> None:
    client = boto3.client("lambda", region_name=REGION)
    client.invoke(
        FunctionName="dataforge-gold-generator",
        InvocationType="Event",
        Payload=json.dumps({}),
    )
    print("Triggered dataforge-gold-generator.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild Silver active layer from Bronze history.")
    parser.add_argument("--days", type=int, default=21, help="Days of Bronze history to merge.")
    parser.add_argument("--skip-gold", action="store_true", help="Do not trigger gold generator.")
    args = parser.parse_args()

    print(f"Loading Bronze history for the last {args.days} days...")
    bronze_df = load_bronze_history(BRONZE_BUCKET, days=args.days)
    if bronze_df.empty:
        print("No Bronze data found. Aborting.")
        return 1

    print(f"Rebuilding active Silver with {len(bronze_df)} jobs (force_rebuild=True)...")
    process_scd_type_2(
        bronze_df,
        SILVER_PATH,
        gold_bucket=GOLD_BUCKET,
        force_rebuild=True,
    )

    if not args.skip_gold:
        invoke_gold_generator()

    print("Silver rebuild complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

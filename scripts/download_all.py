"""Download all Gold CSV snapshots from S3 into data/gold/."""

import os
import sys
import traceback
from pathlib import Path

import awswrangler as wr

sys.path.insert(0, str(Path(__file__).resolve().parent))
from paths import GOLD_DIR

GOLD_BUCKET = "s3://dataforge-gold-dev-eu-central-1"

GOLD_FILES = [
    "all_jobs.csv",
    "expired_jobs.csv",
    "jobs_by_source.csv",
    "jobs_by_region.csv",
    "top_locations.csv",
    "top_companies.csv",
    "remote_vs_onsite.csv",
    "jobs_trend.csv",
    "active_vs_expired.csv",
    "top_skills.csv",
    "description_insights.csv",
    "pipeline_stats.csv",
]


def main():
    print("Downloading Gold CSVs from S3...")
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    for name in GOLD_FILES:
        df = wr.s3.read_csv(f"{GOLD_BUCKET}/{name}")
        out = GOLD_DIR / name
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"  {name}: {len(df)} rows")

    print(f"\nSaved {len(GOLD_FILES)} files to {GOLD_DIR}")


if __name__ == "__main__":
    try:
        key_id = os.environ.get("AWS_ACCESS_KEY_ID", "")
        region = os.environ.get("AWS_DEFAULT_REGION", "")
        print(f"AWS region: {region or 'MISSING'} | access key: {'set' if key_id else 'MISSING'}")
        main()
    except Exception:
        print("\nERROR: Failed to download Gold data from S3.", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)

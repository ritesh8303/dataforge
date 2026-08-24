import os
import pandas as pd
from datetime import datetime, timezone
from processing.fetchers import BerlinStartupJobsFetcher


def lambda_handler(event, context):
    """
    Fetch jobs from Berlin Startup Jobs and store them in the Bronze bucket.
    """
    bucket = os.environ.get("BRONZE_BUCKET")
    is_local = os.environ.get("LOCAL_RUN") == "true"

    try:
        if not bucket and not is_local:
            raise ValueError("BRONZE_BUCKET environment variable is not set.")
        fetcher = BerlinStartupJobsFetcher()
        data = fetcher.fetch_jobs()
        df = pd.DataFrame(data["data"])

        if df.empty:
            print("No jobs found from Berlin Startup Jobs.")
            return {"statusCode": 204, "body": "No jobs found to ingest."}

        # --- Normalize columns ---

        # Stringify list columns so Parquet stays flat
        for col in ["tags", "job_types"]:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: ",".join(x) if isinstance(x, list) else str(x))

        # Ensure correct boolean type for remote
        if "remote" in df.columns:
            df["remote"] = df["remote"].astype(bool)

        # Add source and ingestion timestamp
        df["source"] = "berlin_startups"
        df["ingested_at"] = datetime.now(timezone.utc).isoformat()

        # Partition by date
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        path = f"s3://{bucket}/berlin_startups/ingested_at={date_str}/jobs.parquet"

        from processing.utils import save_parquet
        save_parquet(df, path, "berlin_startups")
        print(f"Successfully ingested {len(df)} jobs from Berlin Startup Jobs.")
        return {"statusCode": 200, "body": f"Successfully ingested {len(df)} jobs."}

    except Exception as e:
        # Re-raise so EventBridge marks the invocation failed and the DLQ/alarms fire.
        print(f"Berlin Startup Jobs ingestion failed: {str(e)}")
        raise

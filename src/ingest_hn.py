import os
import json
import pandas as pd
import awswrangler as wr
from datetime import datetime, timezone
from processing.fetchers import HackerNewsFetcher


def lambda_handler(event, context):
    """
    Fetch jobs from Hacker News (jobstories + Who is Hiring comments) and store them in the Bronze bucket.
    """
    bucket = os.environ.get('BRONZE_BUCKET')

    try:
        if not bucket:
            raise ValueError("BRONZE_BUCKET environment variable is not set.")
        fetcher = HackerNewsFetcher()
        data = fetcher.fetch_jobs()
        df = pd.DataFrame(data['data'])

        if df.empty:
            print("No jobs found from Hacker News.")
            return {"statusCode": 204, "body": "No jobs found to ingest."}

        # --- Normalize columns ---

        # Stringify list columns so Parquet stays flat
        for col in ['tags', 'job_types']:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: ','.join(x) if isinstance(x, list) else str(x)
                )

        # Ensure correct boolean type for remote
        if 'remote' in df.columns:
            df['remote'] = df['remote'].astype(bool)

        # Add source and ingestion timestamp
        df['source'] = 'hacker_news'
        df['ingested_at'] = datetime.now(timezone.utc).isoformat()

        # Partition by date
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        path = f"s3://{bucket}/hacker_news/ingested_at={date_str}/jobs.parquet"

        wr.s3.to_parquet(df=df, path=path, index=False)
        print(f"Successfully ingested {len(df)} jobs from Hacker News.")
        return {"statusCode": 200, "body": f"Successfully ingested {len(df)} jobs."}

    except Exception as e:
        error_msg = f"Hacker News ingestion failed: {str(e)}"
        print(error_msg)
        return {"statusCode": 500, "body": json.dumps({"error": error_msg})}

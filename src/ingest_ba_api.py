import os
import json
import pandas as pd
import awswrangler as wr
from datetime import datetime, timezone
from processing.fetchers import BAFetcher


def lambda_handler(event, context):
    """
    Fetch jobs from BA API using OAuth2 and store them in the Bronze bucket.
    """
    bucket = os.environ.get('BRONZE_BUCKET')

    try:
        if not bucket:
            raise ValueError("BRONZE_BUCKET environment variable is not set.")
        fetcher = BAFetcher()
        data = fetcher.fetch_jobs(query="Data Engineer")
        df = pd.DataFrame(data['stellenangebote'])

        if df.empty:
            print("No jobs found from BA API.")
            return {"statusCode": 204, "body": "No jobs found from BA API."}

        # --- Normalize columns ---

        # Flatten nested arbeitsort dict into separate columns
        if 'arbeitsort' in df.columns:
            arbeitsort_df = df['arbeitsort'].apply(
                lambda x: x if isinstance(x, dict) else {}
            ).apply(pd.Series)
            df['location'] = arbeitsort_df.get('city', pd.Series([''] * len(df)))
            df['zip_code'] = arbeitsort_df.get('zip_code', pd.Series([''] * len(df)))
            df['state'] = arbeitsort_df.get('state', pd.Series([''] * len(df)))
            df.drop(columns=['arbeitsort'], inplace=True)

        # Rename German field names to unified English schema
        df.rename(columns={
            'refnr':              'job_id',
            'titel':              'title',
            'arbeitgeber':        'company',
            'eintrittsdatum':     'start_date_raw',
            'modifikationsdatum': 'modified_at',
        }, inplace=True)

        # Add source and ingestion timestamp
        df['source'] = 'ba_api'
        df['ingested_at'] = datetime.now(timezone.utc).isoformat()

        # Partition by date
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        path = f"s3://{bucket}/ba_api/ingested_at={date_str}/jobs.parquet"

        wr.s3.to_parquet(df=df, path=path, index=False)
        print(f"Successfully ingested {len(df)} jobs from BA API.")
        return {"statusCode": 200, "body": f"Successfully ingested {len(df)} jobs from BA."}

    except Exception as e:
        error_msg = f"BA API ingestion failed: {str(e)}"
        print(error_msg)
        return {"statusCode": 500, "body": json.dumps({"error": error_msg})}

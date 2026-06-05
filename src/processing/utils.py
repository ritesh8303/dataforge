import os
import awswrangler as wr
from datetime import datetime, timezone

def save_parquet(df, path, source):
    """
    Saves a DataFrame as Parquet.
    If the LOCAL_RUN="true" environment variable is set, it writes to a local folder `data/bronze/`.
    Otherwise, it writes to S3 using awswrangler.
    """
    if os.environ.get("LOCAL_RUN") == "true":
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        local_dir = os.path.join("data", "bronze", source, f"ingested_at={date_str}")
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, "jobs.parquet")
        df.to_parquet(local_path, index=False)
        print(f"Successfully saved {len(df)} jobs locally to: {local_path}")
        return local_path
    else:
        wr.s3.to_parquet(df=df, path=path, index=False)
        print(f"Successfully saved {len(df)} jobs to S3: {path}")
        return path

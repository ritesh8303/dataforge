import awswrangler as wr
import pandas as pd

print("Reading Silver data from S3...")
silver_path = "s3://dataforge-silver-dev-eu-central-1/cleaned/jobs_history.parquet/"
active_path = f"{silver_path}is_current=True/"
inactive_path = f"{silver_path}is_current=False/"

dfs = []
try:
    if wr.s3.list_objects(path=active_path):
        df_active = wr.s3.read_parquet(path=active_path, dataset=True)
        df_active["is_current"] = True
        dfs.append(df_active)
except Exception as e:
    print(f"Warning: Failed to read active partition: {e}")

try:
    if wr.s3.list_objects(path=inactive_path):
        df_inactive = wr.s3.read_parquet(path=inactive_path, dataset=True)
        df_inactive["is_current"] = False
        dfs.append(df_inactive)
except Exception as e:
    print(f"Warning: Failed to read inactive partition: {e}")

if not dfs:
    raise ValueError("No Silver data found in S3.")

df = pd.concat(dfs, ignore_index=True)
df["is_current"] = df["is_current"].astype(bool)

current = df[df["is_current"] == True].copy()
cols = [
    c for c in ["job_id", "title", "company", "location", "source", "scd_start_date", "remote"] if c in current.columns
]
current = current[cols].reset_index(drop=True)

output = "analytics/all_jobs.csv"
current.to_csv(output, index=False, encoding="utf-8-sig")

print(f"Saved {len(current)} active jobs to {output}")
print(f"Columns: {list(current.columns)}")
print("\nSample data:")
print(current.head(10).to_string())

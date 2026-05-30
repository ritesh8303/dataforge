import awswrangler as wr
import pandas as pd

SILVER_PATH = "s3://dataforge-silver-dev-eu-central-1/cleaned/jobs_history.parquet/"
GOLD_BUCKET = "s3://dataforge-gold-dev-eu-central-1"

print("Reading Silver data from S3...")
df = wr.s3.read_parquet(SILVER_PATH, dataset=True)
current = df[df["is_current"] == True].copy().reset_index(drop=True)
print(f"Active jobs: {len(current)}, Total records (inc. history): {len(df)}")

# Save full active jobs
cols = [c for c in ["job_id", "title", "company", "location", "source", "scd_start_date", "remote"] if c in current.columns]
all_jobs = current[cols].copy()
all_jobs["date_added"] = pd.to_datetime(all_jobs["scd_start_date"]).dt.date.astype(str)
all_jobs.drop(columns=["scd_start_date"], inplace=True)
all_jobs.to_csv("analytics/all_jobs.csv", index=False, encoding="utf-8-sig")
print(f"Saved {len(all_jobs)} rows to analytics/all_jobs.csv")

# Save full history (including expired)
history = df[[c for c in ["job_id", "title", "company", "location", "source", "scd_start_date", "scd_end_date", "is_current", "hash_key"] if c in df.columns]].copy()
history["scd_start_date"] = pd.to_datetime(history["scd_start_date"], errors='coerce').dt.date.astype(str)
history["scd_end_date"] = pd.to_datetime(history["scd_end_date"], errors='coerce').dt.date.astype(str)
history.to_csv("analytics/full_history.csv", index=False, encoding="utf-8-sig")
print(f"Saved {len(history)} rows to analytics/full_history.csv")

# Download all gold CSVs from S3
print("\nDownloading gold CSVs from S3...")
gold_files = ["all_jobs.csv", "top_locations.csv", "top_companies.csv",
              "jobs_by_source.csv", "remote_vs_onsite.csv", "jobs_trend.csv", "active_vs_expired.csv"]

for f in gold_files:
    df_gold = wr.s3.read_csv(f"{GOLD_BUCKET}/{f}")
    df_gold.to_csv(f"analytics/{f}", index=False, encoding="utf-8-sig")
    print(f"  {f}: {len(df_gold)} rows")

print("\nAll files saved to analytics/ folder.")
print("Open them directly in Excel.")

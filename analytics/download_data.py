import awswrangler as wr

print("Reading Silver data from S3...")
df = wr.s3.read_parquet(
    "s3://dataforge-silver-dev-eu-central-1/cleaned/jobs_history.parquet/",
    dataset=True
)

current = df[df["is_current"] == True].copy()
cols = [c for c in ["job_id", "title", "company", "location", "source", "scd_start_date", "remote"] if c in current.columns]
current = current[cols].reset_index(drop=True)

output = "analytics/all_jobs.csv"
current.to_csv(output, index=False, encoding="utf-8-sig")

print(f"Saved {len(current)} active jobs to {output}")
print(f"Columns: {list(current.columns)}")
print("\nSample data:")
print(current.head(10).to_string())

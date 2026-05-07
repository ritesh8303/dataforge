import awswrangler as wr
import pandas as pd

GOLD_BUCKET = "s3://dataforge-gold-dev-eu-central-1"
SILVER_PATH = "s3://dataforge-silver-dev-eu-central-1/cleaned/jobs_history.parquet/"

print("Reading Silver data from S3...")
df = wr.s3.read_parquet(SILVER_PATH, dataset=True)
current = df[df["is_current"] == True].copy().reset_index(drop=True)
print(f"Total active jobs: {len(current)}")

# 1. All jobs — full detail for Looker Studio
all_jobs = current[[c for c in ["job_id", "title", "company", "location", "source", "scd_start_date", "remote"] if c in current.columns]].copy()
all_jobs["date_added"] = pd.to_datetime(all_jobs["scd_start_date"]).dt.date.astype(str)
all_jobs.drop(columns=["scd_start_date"], inplace=True)

# 2. Jobs by source
jobs_by_source = current.groupby("source").size().reset_index(name="job_count").sort_values("job_count", ascending=False)

# 3. Top locations — clean up duplicates like "Berlin, Berlin, Germany" → "Berlin"
current["location_clean"] = current["location"].str.split(",").str[0].str.strip()
top_locations = (
    current[current["location_clean"].notna() & (current["location_clean"] != "")]
    .groupby("location_clean").size()
    .reset_index(name="job_count")
    .sort_values("job_count", ascending=False)
    .head(20)
    .rename(columns={"location_clean": "location"})
)

# 4. Remote vs onsite (Arbeitnow only)
if "remote" in current.columns:
    remote_df = current[current["source"] == "arbeitnow"].copy()
    remote_df["work_type"] = remote_df["remote"].apply(lambda x: "Remote" if x is True or x == "True" else "On-site")
    remote_vs_onsite = remote_df.groupby("work_type").size().reset_index(name="job_count")
else:
    remote_vs_onsite = pd.DataFrame({"work_type": [], "job_count": []})

# 5. Jobs trend over time
all_records = df.copy()
all_records["date"] = pd.to_datetime(all_records["scd_start_date"]).dt.date.astype(str)
jobs_trend = all_records.groupby("date").size().reset_index(name="new_jobs").sort_values("date")

# 6. Top companies
top_companies = (
    current[current["company"].notna() & (current["company"] != "")]
    .groupby("company").size()
    .reset_index(name="job_count")
    .sort_values("job_count", ascending=False)
    .head(20)
)

# 7. Active vs expired
status = df.copy()
status["status"] = status["is_current"].apply(lambda x: "Active" if x else "Expired")
active_vs_expired = status.groupby("status").size().reset_index(name="job_count")

# Print summaries
print(f"\nJobs by source:\n{jobs_by_source.to_string(index=False)}")
print(f"\nTop 10 locations:\n{top_locations.head(10).to_string(index=False)}")
print(f"\nRemote vs onsite:\n{remote_vs_onsite.to_string(index=False)}")
print(f"\nJobs trend:\n{jobs_trend.to_string(index=False)}")
print(f"\nTop 10 companies:\n{top_companies.head(10).to_string(index=False)}")
print(f"\nActive vs expired:\n{active_vs_expired.to_string(index=False)}")

# Write to S3
print("\nWriting Gold outputs to S3...")
wr.s3.to_csv(all_jobs,          path=f"{GOLD_BUCKET}/all_jobs.csv",           index=False)
wr.s3.to_csv(jobs_by_source,    path=f"{GOLD_BUCKET}/jobs_by_source.csv",     index=False)
wr.s3.to_csv(top_locations,     path=f"{GOLD_BUCKET}/top_locations.csv",      index=False)
wr.s3.to_csv(remote_vs_onsite,  path=f"{GOLD_BUCKET}/remote_vs_onsite.csv",   index=False)
wr.s3.to_csv(jobs_trend,        path=f"{GOLD_BUCKET}/jobs_trend.csv",         index=False)
wr.s3.to_csv(top_companies,     path=f"{GOLD_BUCKET}/top_companies.csv",      index=False)
wr.s3.to_csv(active_vs_expired, path=f"{GOLD_BUCKET}/active_vs_expired.csv",  index=False)
print("Gold layer written to S3 successfully.")

# Also save locally
print("\nSaving locally to analytics/...")
all_jobs.to_csv("analytics/all_jobs.csv",                   index=False, encoding="utf-8-sig")
jobs_by_source.to_csv("analytics/jobs_by_source.csv",       index=False, encoding="utf-8-sig")
top_locations.to_csv("analytics/top_locations.csv",         index=False, encoding="utf-8-sig")
remote_vs_onsite.to_csv("analytics/remote_vs_onsite.csv",   index=False, encoding="utf-8-sig")
jobs_trend.to_csv("analytics/jobs_trend.csv",               index=False, encoding="utf-8-sig")
top_companies.to_csv("analytics/top_companies.csv",         index=False, encoding="utf-8-sig")
active_vs_expired.to_csv("analytics/active_vs_expired.csv", index=False, encoding="utf-8-sig")
print("All files saved locally.")

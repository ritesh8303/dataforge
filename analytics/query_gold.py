import duckdb
import pandas as pd
import awswrangler as wr

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

GOLD_BUCKET = "s3://dataforge-gold-dev-eu-central-1"
SILVER_PATH = "s3://dataforge-silver-dev-eu-central-1/cleaned/jobs_history.parquet/"

# 1. Initialize DuckDB with S3 support
con = duckdb.connect()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute("CALL load_aws_credentials();")
con.execute("SET s3_region='eu-central-1';")

print("🔍 Connecting to Silver Layer...")
con.execute(f"CREATE VIEW consolidated_jobs AS SELECT * FROM read_parquet('{SILVER_PATH}', union_by_name=True);")

# Only query current records for gold outputs
BASE_FILTER = "WHERE is_current = true"

# 2. Jobs by source
print("\n--- JOB COUNT BY SOURCE ---")
df_source = con.execute(f"""
    SELECT source, COUNT(*) as job_count
    FROM consolidated_jobs {BASE_FILTER}
    GROUP BY source ORDER BY job_count DESC
""").df()
print(df_source)

# 3. Top locations
print("\n--- TOP JOB LOCATIONS ---")
df_loc = con.execute(f"""
    SELECT COALESCE(location, 'Remote/Unknown') as location, COUNT(*) as job_count
    FROM consolidated_jobs {BASE_FILTER}
    AND location IS NOT NULL
    GROUP BY location ORDER BY job_count DESC LIMIT 20
""").df()
print(df_loc)

# 4. Remote vs onsite
print("\n--- REMOTE VS ONSITE ---")
df_remote = con.execute(f"""
    SELECT
        CASE WHEN remote = true THEN 'Remote' ELSE 'On-site' END as work_type,
        COUNT(*) as job_count
    FROM consolidated_jobs {BASE_FILTER}
    AND remote IS NOT NULL
    GROUP BY work_type
""").df()
print(df_remote)

# 5. Jobs added over time (trend)
print("\n--- JOBS ADDED OVER TIME ---")
df_trend = con.execute(f"""
    SELECT
        CAST(scd_start_date AS DATE) as date,
        COUNT(*) as new_jobs
    FROM consolidated_jobs
    WHERE is_current = true OR is_current = false
    GROUP BY date ORDER BY date
""").df()
print(df_trend)

# 6. Write all gold tables to S3 as CSV (Looker Studio compatible)
print("\n📤 Writing Gold outputs to S3...")
wr.s3.to_csv(df_source, path=f"{GOLD_BUCKET}/jobs_by_source.csv", index=False)
wr.s3.to_csv(df_loc,    path=f"{GOLD_BUCKET}/top_locations.csv",  index=False)
wr.s3.to_csv(df_remote, path=f"{GOLD_BUCKET}/remote_vs_onsite.csv", index=False)
wr.s3.to_csv(df_trend,  path=f"{GOLD_BUCKET}/jobs_trend.csv",      index=False)
print("✅ Gold layer written to S3 successfully.")

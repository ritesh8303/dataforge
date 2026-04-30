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

print("Connecting to Silver Layer...")
con.execute(f"CREATE VIEW consolidated_jobs AS SELECT * FROM read_parquet('{SILVER_PATH}**/*.parquet', union_by_name=True);")

# Only query current records for gold outputs
BASE_FILTER = "WHERE is_current = true"

# 2. Jobs by location (no source column in current silver)
print("\n--- TOP JOB LOCATIONS ---")
df_source = con.execute(f"""
    SELECT COALESCE(location, 'Remote/Unknown') as location, COUNT(*) as job_count
    FROM consolidated_jobs {BASE_FILTER}
    GROUP BY location ORDER BY job_count DESC LIMIT 20
""").df()
print(df_source)

# 3. Top locations (same as above, alias for Looker)
df_loc = df_source.copy()

# 4. Jobs added over time (trend)
print("\n--- JOBS ADDED OVER TIME ---")
df_trend = con.execute("""
    SELECT
        CAST(scd_start_date AS DATE) as date,
        COUNT(*) as new_jobs
    FROM consolidated_jobs
    GROUP BY date ORDER BY date
""").df()
print(df_trend)

# 5. Current vs historical records
print("\n--- CURRENT VS HISTORICAL ---")
df_status = con.execute("""
    SELECT
        CASE WHEN is_current = true THEN 'Active' ELSE 'Expired' END as status,
        COUNT(*) as job_count
    FROM consolidated_jobs
    GROUP BY status
""").df()
print(df_status)

# 6. Write all gold tables to S3 as CSV (Looker Studio compatible)
print("Writing Gold outputs to S3...")
wr.s3.to_csv(df_loc,    path=f"{GOLD_BUCKET}/top_locations.csv",      index=False)
wr.s3.to_csv(df_trend,  path=f"{GOLD_BUCKET}/jobs_trend.csv",         index=False)
wr.s3.to_csv(df_status, path=f"{GOLD_BUCKET}/active_vs_expired.csv",  index=False)
print("Gold layer written to S3 successfully.")

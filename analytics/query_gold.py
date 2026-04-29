import duckdb
import boto3
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# 1. Initialize DuckDB with S3 support
con = duckdb.connect()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Load your local AWS keys
con.execute("CALL load_aws_credentials();")

# 2. Set up AWS Region
con.execute("SET s3_region='eu-central-1';")

print("🔍 Connecting to Data Lakehouse (Silver Layer)...")

# 3. Define the path and create the View with Union support
# Pointing to the root of the partitioned dataset
silver_path = "s3://dataforge-silver-dev-eu-central-1/cleaned/jobs_history.parquet/"
con.execute(f"CREATE VIEW consolidated_jobs AS SELECT * FROM read_parquet('{silver_path}', union_by_name=True);")

# 4. Run your Gold Insights
print("\n--- JOB COUNT BY SOURCE (Polished) ---")
res_counts = con.execute("""
    SELECT 
        source, 
        COUNT(*) as job_count 
    FROM consolidated_jobs 
    GROUP BY source
    ORDER BY job_count DESC
""").df()
print(res_counts)

print("\n--- SAMPLE JOBS (Unified Schema) ---")
res_sample = con.execute("""
    SELECT 
        title, 
        company, 
        location, 
        source 
    FROM consolidated_jobs 
    LIMIT 5
""").df()
print(res_sample)

print("\n--- ANALYTICS: Most common Job Locations ---")
res_loc = con.execute("""
    SELECT location, COUNT(*) as frequency 
    FROM consolidated_jobs 
    WHERE location IS NOT NULL
    GROUP BY location 
    ORDER BY frequency DESC 
    LIMIT 5
""").df()
print(res_loc)
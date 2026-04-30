import duckdb
import matplotlib.pyplot as plt
import pandas as pd

# 1. Connect to DuckDB and load S3 extensions
con = duckdb.connect()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Load local AWS credentials
con.execute("CALL load_aws_credentials();")
con.execute("SET s3_region='eu-central-1';")

print("🔍 Pulling unified data from Silver Layer for visualization...")

# 2. Create the unified view
silver_path = "s3://dataforge-silver-dev-eu-central-1/cleaned/jobs_history.parquet/"
con.execute(f"CREATE VIEW consolidated_jobs AS SELECT * FROM read_parquet('{silver_path}', union_by_name=True);")

# 3. Query: Top 5 Job Locations (current records only)
df_loc = con.execute("""
    SELECT
        COALESCE(location, 'Remote/Unknown') as city,
        COUNT(*) as job_count
    FROM consolidated_jobs
    WHERE is_current = true AND location IS NOT NULL
    GROUP BY city
    ORDER BY job_count DESC
    LIMIT 5
""").df()

# 4. Create the Visualization
plt.figure(figsize=(12, 7))
bars = plt.bar(df_loc['city'], df_loc['job_count'], color='#3498db', edgecolor='#2980b9')

# Add labels and styling
plt.title('Top 5 German Cities for Data Engineers', fontsize=16, fontweight='bold', pad=20)
plt.xlabel('City', fontsize=12)
plt.ylabel('Number of Job Postings', fontsize=12)
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.6)

# Add data labels on top of bars
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 0.5, yval, ha='center', va='bottom', fontweight='bold')

plt.tight_layout()

# 5. Save the result
output_file = 'job_market_overview.png'
plt.savefig(output_file)
print(f"✅ Success! Chart saved as: {output_file}")
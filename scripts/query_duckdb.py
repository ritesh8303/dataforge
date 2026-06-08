"""
Query Gold CSVs locally using DuckDB — no AWS credentials needed.
Usage: python scripts/query_duckdb.py
"""

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from paths import GOLD_DIR

gold = str(GOLD_DIR).replace("\\", "/")
con = duckdb.connect()

print("=== Top 10 Cities ===")
print(
    con.execute(f"""
    SELECT location, job_count
    FROM '{gold}/top_locations.csv'
    ORDER BY job_count DESC LIMIT 10
""")
    .df()
    .to_string(index=False)
)

print("\n=== Jobs by Source ===")
print(
    con.execute(f"""
    SELECT source, job_count,
           ROUND(100.0 * job_count / SUM(job_count) OVER (), 1) AS pct
    FROM '{gold}/jobs_by_source.csv'
""")
    .df()
    .to_string(index=False)
)

print("\n=== Top 10 Companies ===")
print(
    con.execute(f"""
    SELECT company, job_count
    FROM '{gold}/top_companies.csv'
    ORDER BY job_count DESC LIMIT 10
""")
    .df()
    .to_string(index=False)
)

print("\n=== Jobs Trend (last 7 days) ===")
print(
    con.execute(f"""
    SELECT date, new_jobs
    FROM '{gold}/jobs_trend.csv'
    ORDER BY date DESC LIMIT 7
""")
    .df()
    .to_string(index=False)
)

print("\n=== Active vs Expired ===")
print(
    con.execute(f"""
    SELECT status, job_count,
           ROUND(100.0 * job_count / SUM(job_count) OVER (), 1) AS pct
    FROM '{gold}/active_vs_expired.csv'
""")
    .df()
    .to_string(index=False)
)

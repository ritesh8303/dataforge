"""
Query Gold CSVs locally using DuckDB — no AWS credentials needed.
Usage: python analytics/query_duckdb.py
"""
import duckdb

con = duckdb.connect()

print("=== Top 10 Cities ===")
print(con.execute("""
    SELECT location, job_count
    FROM 'analytics/top_locations.csv'
    ORDER BY job_count DESC LIMIT 10
""").df().to_string(index=False))

print("\n=== Jobs by Source ===")
print(con.execute("""
    SELECT source, job_count,
           ROUND(100.0 * job_count / SUM(job_count) OVER (), 1) AS pct
    FROM 'analytics/jobs_by_source.csv'
""").df().to_string(index=False))

print("\n=== Top 10 Companies ===")
print(con.execute("""
    SELECT company, job_count
    FROM 'analytics/top_companies.csv'
    ORDER BY job_count DESC LIMIT 10
""").df().to_string(index=False))

print("\n=== Jobs Trend (last 7 days) ===")
print(con.execute("""
    SELECT date, new_jobs
    FROM 'analytics/jobs_trend.csv'
    ORDER BY date DESC LIMIT 7
""").df().to_string(index=False))

print("\n=== Active vs Expired ===")
print(con.execute("""
    SELECT status, job_count,
           ROUND(100.0 * job_count / SUM(job_count) OVER (), 1) AS pct
    FROM 'analytics/active_vs_expired.csv'
""").df().to_string(index=False))

"""Quick check of S3 Gold CSVs and live metrics API."""
import csv
import io
import json
import urllib.request

import boto3

s3 = boto3.client("s3", region_name="eu-central-1")
bucket = "dataforge-gold-dev-eu-central-1"

obj = s3.get_object(Bucket=bucket, Key="jobs_by_source.csv")
rows = list(csv.DictReader(io.StringIO(obj["Body"].read().decode("utf-8-sig"))))
print("S3 jobs_by_source:")
for r in rows:
    print(f"  {r['source']}: {r['job_count']}")
print("total", sum(int(r["job_count"]) for r in rows))

for key in ("description_insights.csv", "remote_vs_onsite.csv", "data_quality_report.csv"):
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8-sig")
    print(f"\n{key}:\n{body.strip()[:500]}")

data = json.loads(urllib.request.urlopen("https://2aww80hwgj.execute-api.eu-central-1.amazonaws.com/", timeout=30).read())
print("\nLive API:")
print("  total_jobs:", data.get("total_jobs"))
print("  english_jobs:", data.get("english_jobs", "MISSING"))
print("  remote_counts:", data.get("remote_counts", "MISSING"))

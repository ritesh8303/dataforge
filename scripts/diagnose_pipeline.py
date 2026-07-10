"""Diagnose pipeline state: Bronze partitions, Gold stats, Lambda logs."""
import json
from datetime import date, timedelta

import boto3

s3 = boto3.client("s3", region_name="eu-central-1")
logs = boto3.client("logs", region_name="eu-central-1")
bronze = "dataforge-bronze-dev-eu-central-1"
gold = "dataforge-gold-dev-eu-central-1"

sources = ["arbeitnow", "ba_api", "company_careers", "berlin_startups", "eures"]

print("=== Bronze partitions (last 7 days) ===")
today = date.today()
for offset in range(7, -1, -1):
    day = (today - timedelta(days=offset)).isoformat()
    found = []
    for src in sources:
        key = f"{src}/ingested_at={day}/jobs.parquet"
        try:
            s3.head_object(Bucket=bronze, Key=key)
            found.append(src)
        except s3.exceptions.ClientError:
            pass
    status = ", ".join(found) if found else "NONE"
    print(f"  {day}: {status}")

print("\n=== pipeline_stats.csv (S3) ===")
obj = s3.get_object(Bucket=gold, Key="pipeline_stats.csv")
print(obj["Body"].read().decode("utf-8-sig").strip())

print("\n=== jobs_trend.csv (S3) ===")
obj = s3.get_object(Bucket=gold, Key="jobs_trend.csv")
print(obj["Body"].read().decode("utf-8-sig").strip())

print("\n=== metrics.json ===")
obj = s3.get_object(Bucket=gold, Key="metrics.json")
m = json.loads(obj["Body"].read())
print(f"  last_updated: {m.get('last_updated')}")
print(f"  trend: {m.get('trend', [])}")
print(f"  pipeline_stats: {m.get('pipeline_stats')}")

print("\n=== Recent Lambda invocations (last 5 days) ===")
lambdas = [
    "dataforge-ingestor",
    "dataforge-ba-ingestor",
    "dataforge-company-ingestor",
    "dataforge-berlin-startups-ingestor",
    "dataforge-transformer",
    "dataforge-gold-generator",
]
for fn in lambdas:
    log_group = f"/aws/lambda/{fn}"
    try:
        streams = logs.describe_log_streams(
            logGroupName=log_group,
            orderBy="LastEventTime",
            descending=True,
            limit=3,
        )
        print(f"\n  {fn}:")
        for stream in streams.get("logStreams", []):
            ts = stream.get("lastEventTimestamp")
            if ts:
                from datetime import datetime, timezone
                dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                print(f"    last log: {dt.isoformat()} ({stream['logStreamName'][:40]}...)")
        if not streams.get("logStreams"):
            print("    (no log streams)")
    except logs.exceptions.ResourceNotFoundException:
        print(f"\n  {fn}: log group not found")
    except Exception as e:
        print(f"\n  {fn}: error - {e}")

print("\n=== Silver partition inspection ===")
import awswrangler as wr

silver = "s3://dataforge-silver-dev-eu-central-1/cleaned/jobs_history.parquet/"
for part in ["is_current=True/", "is_current=False/"]:
    path = silver + part
    objs = wr.s3.list_objects(path=path) or []
    print(f"\n{part}: {len(objs)} file(s)")
    for o in objs[:10]:
        try:
            df = wr.s3.read_parquet(path=o)
            name = o.rsplit("/", 1)[-1]
            print(f"  {name}: {len(df)} rows")
        except Exception as e:
            print(f"  {o}: READ ERROR: {e}")

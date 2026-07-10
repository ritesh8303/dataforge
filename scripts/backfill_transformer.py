"""Backfill transformer for missed dates after SILVER_READ_ANOMALY fix."""
import json
import time

import boto3

lc = boto3.client("lambda", region_name="eu-central-1")
dates = ["2026-07-07", "2026-07-08", "2026-07-09", "2026-07-10"]

for d in dates:
    print(f"=== Transformer for {d} ===")
    t0 = time.time()
    resp = lc.invoke(
        FunctionName="dataforge-transformer",
        InvocationType="RequestResponse",
        Payload=json.dumps({"date": d}).encode(),
    )
    body = json.loads(resp["Payload"].read())
    err = resp.get("FunctionError")
    print(f"  status: {body.get('statusCode')}  error: {err}  ({time.time()-t0:.1f}s)")
    print(f"  body: {body.get('body', body)}")
    if err or body.get("statusCode", 500) >= 400:
        print("  FAILED — stopping backfill")
        break
    print()

print("Waiting 30s for gold-generator S3 trigger...")
time.sleep(30)

s3 = boto3.client("s3", region_name="eu-central-1")
obj = s3.get_object(Bucket="dataforge-gold-dev-eu-central-1", Key="pipeline_stats.csv")
print("\nUpdated pipeline_stats.csv:")
print(obj["Body"].read().decode("utf-8-sig").strip())

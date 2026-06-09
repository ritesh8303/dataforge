"""Run full DataForge pipeline: all ingestors → transformer → verify gold."""
from __future__ import annotations

import csv
import io
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

LAMBDA_CFG = Config(read_timeout=600, connect_timeout=60)
REGION = "eu-central-1"
BRONZE_BUCKET = "dataforge-bronze-dev-eu-central-1"
GOLD_BUCKET = "dataforge-gold-dev-eu-central-1"

INGESTORS = [
    "dataforge-ingestor",
    "dataforge-ba-ingestor",
    "dataforge-company-ingestor",
    "dataforge-berlin-startups-ingestor",
]


def invoke_lambda(lc, name: str, payload: dict | None = None) -> dict:
    t0 = time.time()
    raw = json.dumps(payload or {}).encode()
    resp = lc.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=raw)
    body = json.loads(resp["Payload"].read())
    return {
        "name": name,
        "ok": resp.get("FunctionError") is None and body.get("statusCode", 200) < 400,
        "body": body.get("body", body),
        "status": body.get("statusCode"),
        "sec": round(time.time() - t0, 1),
        "error": resp.get("FunctionError"),
    }


def run_eures() -> dict:
    t0 = time.time()
    os.environ["BRONZE_BUCKET"] = BRONZE_BUCKET
    os.environ["LOCAL_RUN"] = "false"
    import ingest_eures

    result = ingest_eures.lambda_handler({}, None)
    return {
        "name": "eures",
        "ok": result.get("statusCode") == 200,
        "body": result.get("body"),
        "status": result.get("statusCode"),
        "sec": round(time.time() - t0, 1),
    }


def wait_for_gold(s3, timeout_sec: int = 180) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            s3.head_object(Bucket=GOLD_BUCKET, Key="jobs_by_source.csv")
            return True
        except Exception:
            time.sleep(10)
    return False


def read_jobs_by_source(s3) -> list[dict]:
    obj = s3.get_object(Bucket=GOLD_BUCKET, Key="jobs_by_source.csv")
    text = obj["Body"].read().decode("utf-8-sig")
    return list(csv.DictReader(io.StringIO(text)))


def main():
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lc = boto3.client("lambda", region_name=REGION, config=LAMBDA_CFG)
    s3 = boto3.client("s3", region_name=REGION)

    print(f"=== DataForge full pipeline run ({date} UTC) ===\n")

    results = []
    print("PHASE 1 — Ingestors (Lambda)")
    for fn in INGESTORS:
        print(f"  {fn}...")
        r = invoke_lambda(lc, fn)
        results.append(r)
        status = "OK" if r["ok"] else "FAIL"
        print(f"    [{status}] {r['body']} ({r['sec']}s)")

    print("\nPHASE 2 — EURES ingest")
    try:
        r = run_eures()
        results.append(r)
        status = "OK" if r["ok"] else "FAIL"
        print(f"    [{status}] {r['body']} ({r['sec']}s)")
    except Exception as e:
        results.append({"name": "eures", "ok": False, "body": str(e)})
        print(f"    [FAIL] {e}")

    print("\nPHASE 3 — Silver transformer")
    r = invoke_lambda(lc, "dataforge-transformer", {"date": date})
    results.append(r)
    status = "OK" if r["ok"] else "FAIL"
    print(f"    [{status}] {r['body']} ({r['sec']}s)")

    print("\nPHASE 4 — Waiting for Gold (S3 trigger)...")
    if wait_for_gold(s3, 180):
        rows = read_jobs_by_source(s3)
        total = sum(int(r["job_count"]) for r in rows)
        print("    Gold jobs_by_source.csv:")
        for row in rows:
            print(f"      {row['source']}: {row['job_count']}")
        print(f"    TOTAL active jobs: {total}")
    else:
        print("    Gold CSV not refreshed within 3 min (gold generator may still be running)")

    print("\nPHASE 5 — API smoke test")
    import urllib.request

    for label, url in [
        ("metrics", "https://2aww80hwgj.execute-api.eu-central-1.amazonaws.com/"),
        ("jobs", "https://2amv4immb0.execute-api.eu-central-1.amazonaws.com/?limit=1"),
    ]:
        try:
            data = json.loads(urllib.request.urlopen(url, timeout=30).read())
            if label == "metrics":
                print(f"    metrics total_jobs: {data.get('total_jobs')}")
            else:
                print(f"    jobs API total kpi: {data.get('kpis', {}).get('total')}")
        except Exception as e:
            print(f"    {label} API FAILED: {e}")

    failed = [r["name"] for r in results if not r.get("ok")]
    print("\n=== SUMMARY ===")
    if failed:
        print(f"FAILED: {', '.join(failed)}")
        sys.exit(1)
    print("All pipeline steps completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

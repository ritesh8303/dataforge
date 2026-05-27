import os
import csv
import json
import boto3
import time
from io import StringIO
from datetime import date

s3 = boto3.client("s3")

_cache = {"data": None, "ts": 0}
CACHE_TTL = 120  # 2 minutes — matches dashboard refresh interval

CORS_HEADERS = {
    "Access-Control-Allow-Origin": os.environ.get("ALLOWED_ORIGIN", "*"),
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Content-Type": "application/json",
}


def _read_csv(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return list(csv.DictReader(StringIO(obj["Body"].read().decode("utf-8"))))


def _build_payload(bucket):
    all_jobs      = _read_csv(bucket, "all_jobs.csv")
    trend_rows    = _read_csv(bucket, "jobs_trend.csv")
    source_rows   = _read_csv(bucket, "jobs_by_source.csv")
    location_rows = _read_csv(bucket, "top_locations.csv")
    company_rows  = _read_csv(bucket, "top_companies.csv")
    remote_rows   = _read_csv(bucket, "remote_vs_onsite.csv")
    status_rows   = _read_csv(bucket, "active_vs_expired.csv")
    skill_rows    = _read_csv(bucket, "top_skills.csv")

    today = date.today().isoformat()

    jobs_by_source = {r["source"]: int(r["job_count"]) for r in source_rows}

    trend = [
        {"date": r["date"], "count": int(r["new_jobs"])}
        for r in sorted(trend_rows, key=lambda x: x["date"])[-30:]
    ]

    top_locations = [
        {"location": r["location"], "count": int(r["job_count"])}
        for r in location_rows[:10]
    ]

    top_companies = [
        {"company": r["company"], "count": int(r["job_count"])}
        for r in company_rows[:10]
    ]

    remote_vs_onsite = {r["work_type"]: int(r["job_count"]) for r in remote_rows}

    active_vs_expired = {r["status"]: int(r["job_count"]) for r in status_rows}

    top_skills = [
        {"skill": r["skill"], "count": int(r["job_count"])}
        for r in skill_rows[:15]
    ]

    return {
        "total_jobs":        len(all_jobs),
        "new_today":         sum(1 for j in all_jobs if j.get("date_added", "") == today),
        "jobs_by_source":    jobs_by_source,
        "trend":             trend,
        "top_locations":     top_locations,
        "top_companies":     top_companies,
        "remote_vs_onsite":  remote_vs_onsite,
        "active_vs_expired": active_vs_expired,
        "top_skills":         top_skills,
        "last_updated":      __import__("datetime").datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def lambda_handler(event, context):
    if event.get("httpMethod") == "OPTIONS" or event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    bucket = os.environ["GOLD_BUCKET"]

    now = time.time()
    if _cache["data"] is None or (now - _cache["ts"]) > CACHE_TTL:
        _cache["data"] = _build_payload(bucket)
        _cache["ts"] = now

    return {
        "statusCode": 200,
        "headers": CORS_HEADERS,
        "body": json.dumps(_cache["data"]),
    }

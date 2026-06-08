import os
import csv
import json
import boto3
import time
from io import StringIO

s3 = boto3.client("s3")

_cache = {"data": None, "ts": 0}
CACHE_TTL = 300  # 5 minutes

CORS_HEADERS = {
    "Access-Control-Allow-Origin": os.environ.get("ALLOWED_ORIGIN", "*"),
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Content-Type": "application/json",
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
}


def _load_jobs():
    bucket = os.environ["GOLD_BUCKET"]
    key = os.environ.get("GOLD_KEY", "all_jobs.csv")
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8-sig")
    return list(csv.DictReader(StringIO(content)))


def _is_options(event):
    # API Gateway v2 (HTTP API) uses requestContext.http.method
    # API Gateway v1 (REST API) uses httpMethod
    method = event.get("requestContext", {}).get("http", {}).get("method") or event.get("httpMethod") or ""
    return method.upper() == "OPTIONS"


def lambda_handler(event, context):
    if _is_options(event):
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    # Support both API GW v1 (queryStringParameters) and v2 (same key)
    params = event.get("queryStringParameters") or {}
    search = (params.get("search") or "").lower().strip()
    source = (params.get("source") or "").lower().strip()
    remote = (params.get("remote") or "").lower().strip()
    job_type = (params.get("job_type") or "").lower().strip()
    location = (params.get("location") or "").lower().strip()
    experience = (params.get("experience") or "").lower().strip()
    language_req = (params.get("language_req") or "").lower().strip()
    work_style = (params.get("work_style") or "").lower().strip()
    region = (params.get("region") or "").lower().strip()
    sort = (params.get("sort") or "newest").lower().strip()  # newest | oldest
    status = (params.get("status") or "active").lower().strip()  # active | expired
    max_limit = 12000
    limit = min(int(params.get("limit", 500)), max_limit)

    cache_key = status
    now = time.time()
    if _cache.get(cache_key) is None or (now - _cache.get(cache_key + "_ts", 0)) > CACHE_TTL:
        bucket = os.environ["GOLD_BUCKET"]
        key = "expired_jobs.csv" if status == "expired" else "all_jobs.csv"
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8-sig")
        _cache[cache_key] = list(csv.DictReader(StringIO(content)))
        _cache[cache_key + "_ts"] = now

    jobs = _cache[cache_key]

    if search:
        jobs = [
            j
            for j in jobs
            if search in j.get("title", "").lower()
            or search in j.get("company", "").lower()
            or search in j.get("tags", "").lower()
            or search in j.get("location", "").lower()
        ]
    if location:
        jobs = [j for j in jobs if location in j.get("location", "").lower()]
    if source:
        jobs = [j for j in jobs if j.get("source", "").lower() == source]
    if remote == "true":
        jobs = [j for j in jobs if j.get("is_remote", "").lower() == "true"]
    elif remote == "false":
        jobs = [j for j in jobs if j.get("is_remote", "").lower() != "true"]
    if job_type:
        jobs = [j for j in jobs if job_type in j.get("job_types", "").lower()]
    if experience:
        if experience in ("junior", "entry", "entry-level", "entry_level"):
            jobs = [j for j in jobs if "junior / entry level" in j.get("tags", "").lower()]
        elif experience in ("student", "werkstudent", "working_student", "working-student"):
            jobs = [j for j in jobs if "working student" in j.get("tags", "").lower()]
        elif experience in ("intern", "internship", "praktikum"):
            jobs = [j for j in jobs if "internship" in j.get("tags", "").lower()]
        elif experience in ("thesis", "masterarbeit", "bachelorarbeit", "abschlussarbeit"):
            jobs = [j for j in jobs if "master thesis" in j.get("tags", "").lower()]
    if language_req:
        jobs = [
            j
            for j in jobs
            if j.get("language_requirement", "").lower() == language_req
            or (
                # Fallback for legacy CSV data in S3
                not j.get("language_requirement")
                and language_req == "english_only"
                and (j.get("is_english", "").lower() == "true" or j.get("is_english") is True)
            )
        ]
    if work_style:
        jobs = [
            j
            for j in jobs
            if j.get("work_style", "").lower() == work_style
            or (
                # Fallback for legacy CSV data in S3
                not j.get("work_style")
                and (
                    (work_style == "remote" and j.get("is_remote", "").lower() == "true")
                    or (work_style == "onsite" and j.get("is_remote", "").lower() != "true")
                )
            )
        ]
    if region:
        jobs = [j for j in jobs if j.get("region", "").lower() == region]

    # Sort by date
    reverse = sort != "oldest"
    jobs = sorted(jobs, key=lambda j: j.get("date_added", ""), reverse=reverse)

    all_jobs = _cache.get(cache_key, [])
    today = __import__("datetime").date.today().isoformat()
    filtered_count = len(jobs)
    returned_count = min(filtered_count, limit)
    kpis = {
        "total": len(all_jobs),
        "new_today": sum(1 for j in all_jobs if j.get("date_added", "") == today),
        "remote": sum(1 for j in all_jobs if j.get("is_remote", "").lower() == "true"),
        "filtered": filtered_count,
        "returned": returned_count,
        "truncated": filtered_count > returned_count,
        "limit": limit,
    }

    return {
        "statusCode": 200,
        "headers": CORS_HEADERS,
        "body": json.dumps({"jobs": jobs[:limit], "kpis": kpis, "cached_at": _cache["ts"]}),
    }

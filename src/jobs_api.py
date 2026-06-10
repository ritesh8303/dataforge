import os
import csv
import json
import boto3
import time
from io import StringIO

s3 = boto3.client("s3")

_cache = {"data": None, "ts": 0}
CACHE_TTL = 300  # 5 minutes
MAX_LIMIT = 2000

CORS_HEADERS = {
    "Access-Control-Allow-Origin": os.environ.get("ALLOWED_ORIGIN", "*"),
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Content-Type": "application/json",
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
}


def _job_work_style(job):
    """Return normalized work_style for a job (remote / hybrid / onsite)."""
    ws = (job.get("work_style") or "").lower().strip()
    if ws in ("remote", "hybrid", "onsite"):
        return ws
    # Legacy rows without work_style: map is_remote only
    if str(job.get("is_remote", "")).lower() == "true":
        return "remote"
    return "onsite"


def _is_options(event):
    method = event.get("requestContext", {}).get("http", {}).get("method") or event.get("httpMethod") or ""
    return method.upper() == "OPTIONS"


def lambda_handler(event, context):
    if _is_options(event):
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

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
    sort = (params.get("sort") or "newest").lower().strip()
    status = (params.get("status") or "active").lower().strip()

    # Legacy remote=true|false maps to work_style (standard taxonomy)
    if remote == "true" and not work_style:
        work_style = "remote"
    elif remote == "false" and not work_style:
        work_style = "onsite"

    try:
        requested_limit = int(params.get("limit", 500))
    except (TypeError, ValueError):
        requested_limit = 500
    limit = min(max(requested_limit, 0), MAX_LIMIT)
    offset = max(int(params.get("offset", 0)), 0)

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
                not j.get("language_requirement")
                and language_req == "english_only"
                and (j.get("is_english", "").lower() == "true" or j.get("is_english") is True)
            )
        ]
    if work_style:
        jobs = [j for j in jobs if _job_work_style(j) == work_style]
    if region:
        jobs = [j for j in jobs if j.get("region", "").lower() == region]

    reverse = sort != "oldest"
    jobs = sorted(jobs, key=lambda j: j.get("date_added", ""), reverse=reverse)

    all_jobs = _cache.get(cache_key, [])
    today = __import__("datetime").date.today().isoformat()
    filtered_count = len(jobs)
    page = jobs[offset : offset + limit]
    returned_count = len(page)
    has_more = (offset + returned_count) < filtered_count
    kpis = {
        "total": len(all_jobs),
        "new_today": sum(1 for j in all_jobs if j.get("date_added", "") == today),
        "remote": sum(1 for j in all_jobs if _job_work_style(j) == "remote"),
        "filtered": filtered_count,
        "returned": returned_count,
        "truncated": has_more,
        "offset": offset,
        "limit": limit,
        "requested_limit": requested_limit,
        "applied_limit": limit,
    }

    return {
        "statusCode": 200,
        "headers": CORS_HEADERS,
        "body": json.dumps({"jobs": page, "kpis": kpis, "cached_at": _cache["ts"]}),
    }

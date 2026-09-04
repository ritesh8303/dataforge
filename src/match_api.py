"""Match API — semantic job–resume ranking via embedding retrieval."""

import json
import os
import time

import boto3

from ai_gateway.router import ModelRouter
from embedding_index import build_embedding_index, index_from_json, job_text, rank_by_embedding

s3 = boto3.client("s3")

_cache: dict = {"jobs": None, "index": None, "ts": 0}
CACHE_TTL = 300

CORS_HEADERS = {
    "Access-Control-Allow-Origin": os.environ.get("ALLOWED_ORIGIN", "*"),
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Content-Type": "application/json",
    "Cache-Control": "no-cache, no-store, must-revalidate",
}


def _is_options(event):
    method = event.get("requestContext", {}).get("http", {}).get("method") or event.get("httpMethod") or ""
    return method.upper() == "OPTIONS"


def _load_jobs_and_index():
    now = time.time()
    if _cache["jobs"] is not None and (now - _cache["ts"]) < CACHE_TTL:
        return _cache["jobs"], _cache["index"]

    bucket = os.environ.get("GOLD_BUCKET")
    jobs_key = os.environ.get("GOLD_KEY", "all_jobs.csv")
    index_key = os.environ.get("EMBEDDING_INDEX_KEY", "embedding_index.json")
    enrichment_key = os.environ.get("ENRICHMENT_KEY", "ai_job_enrichment.csv")

    import csv
    from io import StringIO

    jobs_obj = s3.get_object(Bucket=bucket, Key=jobs_key)
    jobs = list(csv.DictReader(StringIO(jobs_obj["Body"].read().decode("utf-8"))))

    enrich_map = {}
    try:
        enrich_obj = s3.get_object(Bucket=bucket, Key=enrichment_key)
        for row in csv.DictReader(StringIO(enrich_obj["Body"].read().decode("utf-8"))):
            enrich_map[row.get("job_id", "")] = row
    except Exception:
        pass

    for job in jobs:
        jid = job.get("job_id", "")
        if jid in enrich_map:
            job.update(enrich_map[jid])

    index = []
    try:
        idx_obj = s3.get_object(Bucket=bucket, Key=index_key)
        index = index_from_json(idx_obj["Body"].read().decode("utf-8"))
    except Exception:
        router = ModelRouter()
        index = build_embedding_index(jobs[: int(os.environ.get("INDEX_BUILD_LIMIT", "200"))], router)

    _cache["jobs"] = jobs
    _cache["index"] = index
    _cache["ts"] = now
    return jobs, index


def _parse_body(event):
    body = event.get("body") or "{}"
    if event.get("isBase64Encoded"):
        import base64
        body = base64.b64decode(body).decode("utf-8")
    if isinstance(body, str):
        try:
            return json.loads(body) if body.strip() else {}
        except json.JSONDecodeError:
            return {}
    return body or {}


def lambda_handler(event, context):
    if _is_options(event):
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}
    try:
        return _handle(event)
    except Exception as e:
        print(f"match_api error: {e}")
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)}),
        }


def _handle(event):
    method = (event.get("requestContext", {}).get("http", {}).get("method") or event.get("httpMethod") or "GET").upper()
    params = event.get("queryStringParameters") or {}
    body = _parse_body(event) if method == "POST" else {}

    resume = (body.get("resume") or params.get("resume") or "").strip()
    dream_role = (body.get("dream_role") or params.get("dream_role") or params.get("search") or "").strip()
    location = (body.get("location") or params.get("location") or "").strip()
    method_pref = (body.get("method") or params.get("method") or "embedding").lower()
    try:
        limit = min(int(body.get("limit") or params.get("limit") or 15), 50)
    except (TypeError, ValueError):
        limit = 15

    if not resume and not dream_role:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "resume or dream_role required"}),
        }

    jobs, index = _load_jobs_and_index()
    query = f"{dream_role} {resume} {location}".strip()
    router = ModelRouter()

    if method_pref == "embedding" and index:
        ranked = rank_by_embedding(query, jobs, index, top_k=limit, router=router)
    else:
        # Keyword fallback for A/B baseline comparison
        q = query.lower()
        ranked = []
        for job in jobs:
            text = job_text(job).lower()
            hits = sum(1 for w in q.split() if len(w) > 2 and w in text)
            ranked.append({**job, "match_score": hits * 10, "match_method": "keyword"})
        ranked.sort(key=lambda x: x["match_score"], reverse=True)
        ranked = ranked[:limit]

    return {
        "statusCode": 200,
        "headers": CORS_HEADERS,
        "body": json.dumps({
            "jobs": ranked,
            "count": len(ranked),
            "method": method_pref,
            "cost_summary": router.cost_logger.summary(),
        }),
    }

import json
import os
import time

from metrics_payload import build_metrics_payload, load_metrics_json

_cache = {"data": None, "ts": 0}
CACHE_TTL = 600

CORS_HEADERS = {
    "Access-Control-Allow-Origin": os.environ.get("ALLOWED_ORIGIN", "*"),
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Content-Type": "application/json",
    "Cache-Control": "public, max-age=600",
}


def lambda_handler(event, context):
    if (
        event.get("httpMethod") == "OPTIONS"
        or event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS"
    ):
        return {"statusCode": 200, "headers": CORS_HEADERS, "body": ""}

    try:
        bucket = os.environ["GOLD_BUCKET"]

        now = time.time()
        if _cache["data"] is None or (now - _cache["ts"]) > CACHE_TTL:
            snapshot = load_metrics_json(bucket)
            _cache["data"] = snapshot if snapshot is not None else build_metrics_payload(bucket)
            _cache["ts"] = now

        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps(_cache["data"]),
        }
    except Exception as e:
        # Return CORS headers on failure too, so the browser surfaces a real
        # error instead of an opaque CORS failure when S3/Gold is unavailable.
        print(f"metrics_api request failed: {e}")
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Internal error, please retry shortly."}),
        }

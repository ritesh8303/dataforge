"""HITL review queue — low-confidence agent outputs land in S3 (or local CSV)."""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def enqueue_review(payload: dict[str, Any]) -> str | None:
    """Append a review row. Returns destination URI/path or None on failure."""
    row = {
        "queued_at": datetime.now(timezone.utc).isoformat(),
        "dream_role": payload.get("dream_role", ""),
        "location": payload.get("location", ""),
        "visa_status": payload.get("visa_status", ""),
        "reason": payload.get("hitl_reason", "low_confidence"),
        "job_ids": ",".join(
            str(j.get("job_id")) for j in (payload.get("jobs") or []) if j.get("job_id")
        ),
        "payload_json": json.dumps(payload)[:4000],
    }
    prefix = os.environ.get("HITL_REVIEW_S3_PREFIX", "").strip()
    if prefix.startswith("s3://"):
        try:
            import boto3

            bucket_key = prefix.removeprefix("s3://")
            bucket, _, key_prefix = bucket_key.partition("/")
            key = f"{key_prefix.rstrip('/')}/review_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
            boto3.client("s3").put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(row).encode("utf-8"),
                ContentType="application/json",
            )
            return f"s3://{bucket}/{key}"
        except Exception as exc:  # pragma: no cover
            logger.warning("HITL S3 write failed: %s", exc)
            return None

    # Local fallback for demos/tests
    path = os.environ.get("HITL_REVIEW_LOCAL_PATH", "data/hitl_review_queue.csv")
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        write_header = not os.path.exists(path)
        with open(path, "a", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(row.keys()))
            if write_header:
                writer.writeheader()
            writer.writerow(row)
        return path
    except Exception as exc:  # pragma: no cover
        logger.warning("HITL local write failed: %s", exc)
        return None


def queue_to_csv_string(rows: list[dict[str, Any]]) -> str:
    buf = io.StringIO()
    if not rows:
        return ""
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()

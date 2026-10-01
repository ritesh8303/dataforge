"""Ingest review queue for ambiguous audience classifications (WSJ-style HITL)."""

from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

logger = logging.getLogger(__name__)

DEFAULT_LOCAL = Path("data/hitl_ingest_review_queue.csv")
FIELDS = [
    "queued_at",
    "job_id",
    "title",
    "company",
    "location",
    "source",
    "field",
    "seniority",
    "reasons",
    "payload_json",
]


def enqueue_ingest_review(rows: Sequence[dict[str, Any]], path: str | Path | None = None) -> str:
    """Append uncertain/rejected-for-review jobs to a local CSV (or S3 prefix)."""
    dest = path or os.environ.get("INGEST_REVIEW_LOCAL_PATH") or str(DEFAULT_LOCAL)
    prefix = os.environ.get("INGEST_REVIEW_S3_PREFIX", "").strip()

    prepared = []
    now = datetime.now(timezone.utc).isoformat()
    for job in rows:
        if not job.get("audience_uncertain") and "ambiguous_needs_review" not in (job.get("audience_reject_reasons") or []):
            # Only queue uncertain; hard rejects are dropped silently from product.
            if job.get("audience_accept"):
                continue
            if not job.get("audience_uncertain"):
                continue
        prepared.append(
            {
                "queued_at": now,
                "job_id": job.get("job_id", ""),
                "title": job.get("title", ""),
                "company": job.get("company", ""),
                "location": job.get("location", ""),
                "source": job.get("source", ""),
                "field": job.get("ai_field") or job.get("field", ""),
                "seniority": job.get("ai_seniority") or job.get("seniority", ""),
                "reasons": ",".join(job.get("audience_reject_reasons") or []),
                "payload_json": json.dumps(
                    {
                        k: job.get(k)
                        for k in (
                            "job_id",
                            "title",
                            "url",
                            "job_url",
                            "description",
                            "tags",
                            "location",
                            "company",
                            "source",
                            "audience_reject_reasons",
                        )
                    },
                    ensure_ascii=False,
                    default=str,
                )[:3500],
            }
        )

    if not prepared:
        return ""

    if prefix.startswith("s3://"):
        try:
            import boto3

            bucket_key = prefix.removeprefix("s3://")
            bucket, _, key_prefix = bucket_key.partition("/")
            key = f"{key_prefix.rstrip('/')}/ingest_review_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
            boto3.client("s3").put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(prepared).encode("utf-8"),
                ContentType="application/json",
            )
            return f"s3://{bucket}/{key}"
        except Exception as exc:  # pragma: no cover
            logger.warning("Ingest review S3 write failed: %s", exc)

    path_obj = Path(dest)
    path_obj.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path_obj.exists() or path_obj.stat().st_size == 0
    with path_obj.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerows(prepared)
    return str(path_obj)

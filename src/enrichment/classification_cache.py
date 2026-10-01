"""Content-hash classification cache (local JSON or S3) to avoid re-LLM."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_LOCAL = Path("data/classification_cache.json")


def content_key(title: str, description: str, tags: str = "") -> str:
    blob = f"{title.strip()}\n{(description or '')[:2000]}\n{tags or ''}"
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:24]


class ClassificationCache:
    def __init__(self, path: str | Path | None = None, s3_uri: str | None = None):
        self.path = Path(path or os.environ.get("CLASSIFICATION_CACHE_PATH") or DEFAULT_LOCAL)
        self.s3_uri = (s3_uri if s3_uri is not None else os.environ.get("CLASSIFICATION_CACHE_S3", "")).strip()
        self._mem: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if self.s3_uri.startswith("s3://"):
            try:
                import boto3

                bucket_key = self.s3_uri.removeprefix("s3://")
                bucket, _, key = bucket_key.partition("/")
                raw = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
                data = json.loads(raw)
                self._mem = data.get("entries", data if isinstance(data, dict) else {})
                return
            except Exception as exc:
                logger.info("Classification cache S3 miss: %s", exc)
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                self._mem = data.get("entries", {})
            except Exception as exc:
                logger.warning("Classification cache local load failed: %s", exc)
                self._mem = {}

    def get(self, key: str) -> dict[str, Any] | None:
        row = self._mem.get(key)
        if not row:
            return None
        return dict(row.get("payload") or {})

    def put(self, key: str, payload: dict[str, Any]) -> None:
        self._mem[key] = {"payload": payload, "ts": time.time()}

    def flush(self) -> str:
        body = json.dumps({"version": 1, "entries": self._mem}, ensure_ascii=False)
        if self.s3_uri.startswith("s3://"):
            try:
                import boto3

                bucket_key = self.s3_uri.removeprefix("s3://")
                bucket, _, key = bucket_key.partition("/")
                boto3.client("s3").put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=body.encode("utf-8"),
                    ContentType="application/json",
                )
                return self.s3_uri
            except Exception as exc:
                logger.warning("Classification cache S3 write failed: %s", exc)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(body, encoding="utf-8")
        return str(self.path)

    def __len__(self) -> int:
        return len(self._mem)

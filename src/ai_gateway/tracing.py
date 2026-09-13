"""Frugal LLM tracing: Langfuse (optional) + S3 JSON + CloudWatch EMF-shaped logs."""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator

logger = logging.getLogger(__name__)


@dataclass
class TraceSpan:
    name: str
    trace_id: str
    span_id: str
    started_at: str
    ended_at: str | None = None
    latency_ms: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class TraceLogger:
    def __init__(self, s3_prefix: str | None = None):
        self.s3_prefix = s3_prefix or os.environ.get("LLM_TRACES_S3_PREFIX", "")
        self.spans: list[TraceSpan] = []
        self._langfuse = None
        if os.environ.get("LANGFUSE_PUBLIC_KEY") and os.environ.get("LANGFUSE_SECRET_KEY"):
            try:
                from langfuse import Langfuse  # type: ignore

                self._langfuse = Langfuse(
                    public_key=os.environ["LANGFUSE_PUBLIC_KEY"],
                    secret_key=os.environ["LANGFUSE_SECRET_KEY"],
                    host=os.environ.get("LANGFUSE_HOST", "https://cloud.langfuse.com"),
                )
            except Exception as exc:  # pragma: no cover
                logger.warning("Langfuse init failed: %s", exc)

    @contextmanager
    def span(self, name: str, **metadata: Any) -> Iterator[TraceSpan]:
        trace_id = str(metadata.pop("trace_id", uuid.uuid4()))
        span = TraceSpan(
            name=name,
            trace_id=trace_id,
            span_id=str(uuid.uuid4()),
            started_at=datetime.now(timezone.utc).isoformat(),
            metadata=dict(metadata),
        )
        t0 = time.perf_counter()
        try:
            yield span
        except Exception as exc:
            span.error = str(exc)
            raise
        finally:
            span.latency_ms = round((time.perf_counter() - t0) * 1000, 2)
            span.ended_at = datetime.now(timezone.utc).isoformat()
            self.spans.append(span)
            self._emit_cloudwatch_emf(span)
            self._emit_langfuse(span)
            self._maybe_write_s3(span)

    def _emit_cloudwatch_emf(self, span: TraceSpan) -> None:
        # Embedded Metric Format — CloudWatch extracts metrics from the JSON log line.
        emf = {
            "_aws": {
                "Timestamp": int(time.time() * 1000),
                "CloudWatchMetrics": [
                    {
                        "Namespace": "DataForge/LLM",
                        "Dimensions": [["Agent"]],
                        "Metrics": [
                            {"Name": "LatencyMs", "Unit": "Milliseconds"},
                            {"Name": "Error", "Unit": "Count"},
                        ],
                    }
                ],
            },
            "Agent": span.name,
            "LatencyMs": span.latency_ms or 0.0,
            "Error": 1 if span.error else 0,
            "TraceId": span.trace_id,
            "PromptVersion": span.metadata.get("prompt_version", ""),
            "CostUsd": span.metadata.get("cost_usd", 0.0),
        }
        print(json.dumps(emf))

    def _emit_langfuse(self, span: TraceSpan) -> None:
        if not self._langfuse:
            return
        try:
            self._langfuse.trace(
                id=span.trace_id,
                name=span.name,
                metadata=span.metadata,
            )
        except Exception as exc:  # pragma: no cover
            logger.warning("Langfuse emit failed: %s", exc)

    def _maybe_write_s3(self, span: TraceSpan) -> None:
        if not self.s3_prefix.startswith("s3://"):
            return
        try:
            import boto3

            bucket_key = self.s3_prefix.removeprefix("s3://")
            bucket, _, prefix = bucket_key.partition("/")
            key = f"{prefix.rstrip('/')}/{span.trace_id}/{span.span_id}.json"
            boto3.client("s3").put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(asdict(span)).encode("utf-8"),
                ContentType="application/json",
            )
        except Exception as exc:  # pragma: no cover
            logger.warning("S3 trace write failed: %s", exc)

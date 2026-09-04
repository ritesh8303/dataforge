from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass, field
from typing import Any

from ai_gateway.config import MODEL_PRICING


@dataclass
class UsageRecord:
    task: str
    provider: str
    model_id: str
    input_tokens: int
    output_tokens: int
    latency_ms: float
    cost_usd: float
    success: bool
    timestamp: float = field(default_factory=time.time)
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class CostLogger:
    """In-memory cost/latency logger for thesis evaluation; flush to S3 in Lambda."""

    def __init__(self):
        self.records: list[UsageRecord] = []

    def estimate_cost(self, model_id: str, input_tokens: int, output_tokens: int) -> float:
        pricing = MODEL_PRICING.get(model_id)
        if not pricing:
            return 0.0
        return (input_tokens / 1000.0) * pricing.input_per_1k + (output_tokens / 1000.0) * pricing.output_per_1k

    def log(
        self,
        task: str,
        provider: str,
        model_id: str,
        input_tokens: int,
        output_tokens: int,
        latency_ms: float,
        success: bool = True,
        error: str = "",
    ) -> UsageRecord:
        record = UsageRecord(
            task=task,
            provider=provider,
            model_id=model_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            latency_ms=latency_ms,
            cost_usd=self.estimate_cost(model_id, input_tokens, output_tokens),
            success=success,
            error=error,
        )
        self.records.append(record)
        return record

    def summary(self) -> dict[str, Any]:
        if not self.records:
            return {"total_cost_usd": 0.0, "total_calls": 0, "by_provider": {}, "by_task": {}}
        by_provider: dict[str, dict] = {}
        by_task: dict[str, dict] = {}
        for r in self.records:
            for key, bucket in ((r.provider, by_provider), (r.task, by_task)):
                if key not in bucket:
                    bucket[key] = {"calls": 0, "cost_usd": 0.0, "latency_ms": 0.0, "failures": 0}
                bucket[key]["calls"] += 1
                bucket[key]["cost_usd"] += r.cost_usd
                bucket[key]["latency_ms"] += r.latency_ms
                if not r.success:
                    bucket[key]["failures"] += 1
        return {
            "total_cost_usd": round(sum(r.cost_usd for r in self.records), 6),
            "total_calls": len(self.records),
            "avg_latency_ms": round(sum(r.latency_ms for r in self.records) / len(self.records), 2),
            "by_provider": by_provider,
            "by_task": by_task,
        }

    def to_json(self) -> str:
        return json.dumps({"records": [r.to_dict() for r in self.records], "summary": self.summary()})

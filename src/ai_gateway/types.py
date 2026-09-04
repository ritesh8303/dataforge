from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ProviderResponse:
    text: str
    model_id: str
    provider: str
    input_tokens: int = 0
    output_tokens: int = 0
    latency_ms: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EmbeddingResponse:
    vector: list[float]
    model_id: str
    provider: str
    input_tokens: int = 0
    latency_ms: float = 0.0


@dataclass
class TaskProfile:
    name: str
    preferred_providers: list[str]
    max_latency_ms: int = 5000
    max_cost_usd: float = 0.01
    require_json: bool = False
    prefer_eu_residency: bool = True

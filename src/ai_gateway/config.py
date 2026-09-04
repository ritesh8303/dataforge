from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ModelPricing:
    input_per_1k: float
    output_per_1k: float


# USD per 1k tokens (approximate, for thesis cost logging — update from provider pricing pages)
MODEL_PRICING: dict[str, ModelPricing] = {
    "gpt-4o-mini": ModelPricing(0.00015, 0.0006),
    "gpt-4o": ModelPricing(0.0025, 0.01),
    "text-embedding-3-small": ModelPricing(0.00002, 0.0),
    "claude-3-haiku-20240307": ModelPricing(0.00025, 0.00125),
    "claude-3-5-sonnet-20241022": ModelPricing(0.003, 0.015),
    "amazon.titan-embed-text-v2:0": ModelPricing(0.0001, 0.0),
    "amazon.titan-text-express-v1": ModelPricing(0.0002, 0.0006),
    "local-tfidf": ModelPricing(0.0, 0.0),
    "local-heuristic": ModelPricing(0.0, 0.0),
}

TASK_PROFILES: dict[str, dict] = {
    "enrich": {
        "preferred_providers": ["bedrock", "openai", "anthropic", "local"],
        "max_latency_ms": 15000,
        "max_cost_usd": 0.002,
        "require_json": True,
        "prefer_eu_residency": True,
    },
    "embed": {
        "preferred_providers": ["openai", "bedrock", "local"],
        "max_latency_ms": 3000,
        "max_cost_usd": 0.0001,
        "require_json": False,
        "prefer_eu_residency": True,
    },
    "rerank": {
        "preferred_providers": ["openai", "anthropic", "bedrock", "local"],
        "max_latency_ms": 8000,
        "max_cost_usd": 0.005,
        "require_json": True,
        "prefer_eu_residency": False,
    },
    "summarize": {
        "preferred_providers": ["openai", "bedrock", "anthropic", "local"],
        "max_latency_ms": 10000,
        "max_cost_usd": 0.003,
        "require_json": False,
        "prefer_eu_residency": True,
    },
}


def get_env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def ai_enabled() -> bool:
    return get_env("AI_ENABLED", "true").lower() in ("1", "true", "yes")


def enrichment_sample_rate() -> float:
    try:
        return float(get_env("AI_ENRICHMENT_SAMPLE_RATE", "1.0"))
    except ValueError:
        return 1.0

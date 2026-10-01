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
    # Approximate Bedrock on-demand (update from AWS pricing page for thesis appendix)
    "amazon.nova-micro-v1:0": ModelPricing(0.000035, 0.00014),
    "eu.amazon.nova-micro-v1:0": ModelPricing(0.000035, 0.00014),
    "amazon.nova-lite-v1:0": ModelPricing(0.00006, 0.00024),
    "eu.amazon.nova-lite-v1:0": ModelPricing(0.00006, 0.00024),
    "local-tfidf": ModelPricing(0.0, 0.0),
    "local-heuristic": ModelPricing(0.0, 0.0),
}

TASK_PROFILES: dict[str, dict] = {
    # Production default: OpenAI for all GenAI tasks (Bedrock/Anthropic kept as optional overrides).
    "enrich": {
        "preferred_providers": ["openai", "local"],
        "max_latency_ms": 15000,
        "max_cost_usd": 0.002,
        "require_json": True,
        "prefer_eu_residency": False,
    },
    "embed": {
        "preferred_providers": ["openai", "local"],
        "max_latency_ms": 8000,
        "max_cost_usd": 0.0005,
        "require_json": False,
        "prefer_eu_residency": False,
    },
    "rerank": {
        "preferred_providers": ["openai", "local"],
        "max_latency_ms": 8000,
        "max_cost_usd": 0.005,
        "require_json": True,
        "prefer_eu_residency": False,
    },
    "summarize": {
        "preferred_providers": ["openai", "local"],
        "max_latency_ms": 10000,
        "max_cost_usd": 0.003,
        "require_json": False,
        "prefer_eu_residency": False,
    },
    "explain": {
        "preferred_providers": ["openai", "local"],
        "max_latency_ms": 12000,
        "max_cost_usd": 0.003,
        "require_json": True,
        "prefer_eu_residency": False,
    },
}


def get_env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def resolve_openai_api_key() -> str:
    """Env first, then SSM SecureString (default /dataforge/openai_api_key)."""
    key = get_env("OPENAI_API_KEY")
    if key:
        return key
    param = get_env("OPENAI_API_KEY_SSM", "/dataforge/openai_api_key")
    if not param:
        return ""
    try:
        import boto3

        resp = boto3.client("ssm", region_name=get_env("AWS_DEFAULT_REGION", "eu-central-1")).get_parameter(
            Name=param,
            WithDecryption=True,
        )
        return str(resp.get("Parameter", {}).get("Value") or "").strip()
    except Exception:
        return ""


def ai_enabled() -> bool:
    return get_env("AI_ENABLED", "true").lower() in ("1", "true", "yes")


def daily_budget_usd() -> float:
    try:
        return float(get_env("AI_DAILY_BUDGET_USD", "5.0"))
    except ValueError:
        return 5.0


def enrichment_sample_rate() -> float:
    try:
        return float(get_env("AI_ENRICHMENT_SAMPLE_RATE", "1.0"))
    except ValueError:
        return 1.0

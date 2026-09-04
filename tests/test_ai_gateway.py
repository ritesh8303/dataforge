import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ai_gateway.router import ModelRouter
from ai_gateway.providers.base import validate_json_response
from ai_gateway.cost_logger import CostLogger


def test_local_complete_returns_text():
    router = ModelRouter()
    resp = router.complete("summarize", "Python data engineer role in Berlin with AWS.")
    assert resp.text
    assert resp.provider in ("local", "openai", "anthropic", "bedrock")


def test_local_enrich_returns_valid_json():
    router = ModelRouter()
    resp = router.complete("enrich", "Senior Python AWS data engineer in Berlin", json_mode=True)
    ok, parsed = validate_json_response(resp.text)
    assert ok
    assert "skills" in parsed
    assert parsed["seniority"] in ("junior", "mid", "senior", "lead")


def test_local_embed_deterministic():
    router = ModelRouter()
    a = router.embed("embed", "Python AWS Spark")
    b = router.embed("embed", "Python AWS Spark")
    assert a.vector == b.vector
    assert len(a.vector) > 0


def test_cost_logger_summary():
    logger = CostLogger()
    logger.log("embed", "local", "local-tfidf", 10, 0, 5.0)
    summary = logger.summary()
    assert summary["total_calls"] == 1
    assert summary["total_cost_usd"] == 0.0


def test_router_fallback_on_failure(monkeypatch):
    router = ModelRouter()

    class FailingProvider:
        name = "openai"
        def available(self):
            return True
        def complete(self, *a, **k):
            raise RuntimeError("API down")
        def embed(self, *a, **k):
            raise RuntimeError("API down")

    router._providers["openai"] = FailingProvider()
    resp = router.complete("summarize", "test prompt")
    assert resp.provider == "local"

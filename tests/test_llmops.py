"""Tests for prompt registry, kill switch, and tracing helpers."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from ai_gateway.router import AIDisabledError, ModelRouter
from ai_gateway.tracing import TraceLogger
from prompts.registry import list_prompts, load_prompt


def test_load_enrich_prompt():
    p = load_prompt("enrich", "v1")
    assert p.id == "enrich@v1"
    assert "visa_stance" in p.text
    assert "enrich" in list_prompts()[0] or any("enrich" in x for x in list_prompts())


def test_kill_switch_blocks_router(monkeypatch):
    monkeypatch.setenv("AI_ENABLED", "false")
    router = ModelRouter()
    with pytest.raises(AIDisabledError):
        router.complete("summarize", "hello")


def test_trace_span_records_latency():
    tracer = TraceLogger()
    with tracer.span("test_agent", prompt_version="enrich@v1", cost_usd=0.0) as span:
        span.metadata["ok"] = True
    assert len(tracer.spans) == 1
    assert tracer.spans[0].latency_ms is not None
    assert tracer.spans[0].metadata["ok"] is True

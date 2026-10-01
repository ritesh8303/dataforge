"""Tests for incremental embedding index (content-hash reuse)."""

from __future__ import annotations

from embedding_index import build_embedding_index, content_hash, index_from_json, index_to_json, job_text
from ai_gateway.router import ModelRouter


def test_content_hash_stable():
    assert content_hash("abc") == content_hash("abc")
    assert content_hash("abc") != content_hash("abd")


def test_incremental_reuses_unchanged(monkeypatch):
    router = ModelRouter()
    # Force local embeds
    monkeypatch.setitem(
        router._providers,
        "openai",
        type("X", (), {"name": "openai", "available": lambda self: False})(),
    )
    jobs = [
        {
            "job_id": "j1",
            "title": "Junior Data Engineer",
            "company": "Acme",
            "tags": "python",
            "description": "Python Spark AWS",
        }
    ]
    first = build_embedding_index(jobs, router)
    assert first[0]["content_hash"]
    calls_before = len(router.cost_logger.records)

    second = build_embedding_index(jobs, router, existing=first)
    calls_after = len(router.cost_logger.records)
    assert second[0]["vector"] == first[0]["vector"]
    assert second[0]["content_hash"] == first[0]["content_hash"]
    # No new embed call when hash matches
    assert calls_after == calls_before


def test_incremental_rebuilds_on_change(monkeypatch):
    router = ModelRouter()
    monkeypatch.setitem(
        router._providers,
        "openai",
        type("X", (), {"name": "openai", "available": lambda self: False})(),
    )
    jobs = [
        {
            "job_id": "j1",
            "title": "Junior Data Engineer",
            "company": "Acme",
            "tags": "python",
            "description": "Python Spark AWS",
        }
    ]
    first = build_embedding_index(jobs, router)
    jobs[0]["description"] = "Python Spark AWS Kafka completely new text"
    second = build_embedding_index(jobs, router, existing=first)
    assert second[0]["content_hash"] != first[0]["content_hash"]


def test_index_json_roundtrip():
    entries = [{"job_id": "a", "vector": [0.1, 0.2], "content_hash": "abc", "model": "x", "provider": "local"}]
    raw = index_to_json(entries)
    assert index_from_json(raw)[0]["job_id"] == "a"

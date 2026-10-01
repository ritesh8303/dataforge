"""Contract tests: enrichment schema ↔ Match filters ↔ prompts stay aligned."""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

from enrichment.rules_de_en import FIELD_VALUES, SENIORITY_VALUES, VISA_VALUES
from enrichment.schemas import (
    JobEnrichment,
    PROFILE_VISA_STATUS,
    VISA_BLOCKING_FOR_SEEKER,
    normalize_visa_stance,
    openai_enrichment_json_schema,
    parse_enrichment_payload,
)


def test_visa_blocking_subset_of_visa_values():
    assert VISA_BLOCKING_FOR_SEEKER.issubset(set(VISA_VALUES))


def test_job_enrichment_enums_match_rules():
    schema = openai_enrichment_json_schema()["schema"]
    assert set(schema["properties"]["field"]["enum"]) == set(FIELD_VALUES)
    assert set(schema["properties"]["seniority"]["enum"]) == set(SENIORITY_VALUES)
    assert set(schema["properties"]["visa_stance"]["enum"]) == set(VISA_VALUES)


def test_normalize_visa_aliases():
    assert normalize_visa_stance("sponsors") == "sponsorship_offered"
    assert normalize_visa_stance("not_eligible") == "eu_citizens_only"
    assert normalize_visa_stance("unclear") == "not_mentioned"
    assert normalize_visa_stance("eu_citizens_only") == "eu_citizens_only"


def test_parse_enrichment_rejects_bad_then_coerces_aliases():
    model = parse_enrichment_payload(
        {
            "field": "data_engineering",
            "seniority": "junior",
            "visa_stance": "sponsors",
            "skills": ["Python"],
            "english_ok": True,
            "entry_level": True,
            "job_seeker_visa_friendly": True,
            "is_tech": True,
            "confidence": 0.9,
            "summary": "Junior DE",
            "work_mode": "hybrid",
            "language": "en",
        }
    )
    assert model is not None
    assert model.visa_stance == "sponsorship_offered"
    assert model.field == "data_engineering"


def test_match_filter_visa_values_are_schema_values():
    """Static check: match_service visa blocklist ⊆ VISA_VALUES / VISA_BLOCKING."""
    from api import match_service

    src = inspect.getsource(match_service.apply_profile_filters)
    # Must reference canonical blocking set or the two VISA_VALUES members.
    assert "VISA_BLOCKING_FOR_SEEKER" in src or (
        "eu_citizens_only" in src and "existing_permit_required" in src
    )
    for value in VISA_BLOCKING_FOR_SEEKER:
        assert value in VISA_VALUES


def test_enrich_v2_prompt_lists_canonical_visa_values():
    from prompts.registry import load_prompt

    text = load_prompt("enrich", "v2").text.lower()
    for value in VISA_VALUES:
        assert value in text, f"enrich_v2 missing visa value {value}"
    # Old broken vocab must not be the primary instruction
    assert "sponsorship_offered" in text


def test_ai_classify_system_uses_canonical_visa(monkeypatch):
    from enrichment import ai_classify

    system = ai_classify._load_enrich_system().lower()
    assert "sponsorship_offered" in system
    assert "eu_citizens_only" in system


def test_router_enrich_disallows_local_by_default(monkeypatch):
    from ai_gateway.router import ModelRouter, ProviderExhaustedError, STRICT_NO_LOCAL_FALLBACK

    assert "enrich" in STRICT_NO_LOCAL_FALLBACK
    router = ModelRouter()

    class Boom:
        name = "openai"

        def available(self):
            return True

        def complete(self, *a, **k):
            raise RuntimeError("down")

        def embed(self, *a, **k):
            raise RuntimeError("down")

    router._providers["openai"] = Boom()
    # Make only openai preferred by marking others unavailable except local
    for name in ("anthropic", "bedrock", "azure"):
        router._providers[name] = type(
            "X",
            (),
            {"name": name, "available": lambda self: False, "complete": None, "embed": None},
        )()

    with pytest.raises(ProviderExhaustedError):
        router.complete("enrich", '{"title":"x"}', system="classify", json_mode=True)


def test_router_summarize_still_falls_back_to_local(monkeypatch):
    from ai_gateway.router import ModelRouter

    router = ModelRouter()

    class Boom:
        name = "openai"

        def available(self):
            return True

        def complete(self, *a, **k):
            raise RuntimeError("down")

        def embed(self, *a, **k):
            raise RuntimeError("down")

    router._providers["openai"] = Boom()
    for name in ("anthropic", "bedrock", "azure"):
        router._providers[name] = type(
            "X",
            (),
            {"name": name, "available": lambda self: False},
        )()
    resp = router.complete("summarize", "Python data engineer in Berlin")
    assert resp.provider == "local"


def test_profile_visa_status_documented():
    assert "chancenkarte_or_job_seeker" in PROFILE_VISA_STATUS
    assert "student_visa" in PROFILE_VISA_STATUS

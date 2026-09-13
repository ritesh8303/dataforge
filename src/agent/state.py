"""Shared agent state for the in-process multi-agent match graph."""

from __future__ import annotations

from typing import Any, TypedDict


class AgentState(TypedDict, total=False):
    resume: str
    dream_role: str
    location: str
    visa_status: str
    german_level: str
    entry_level_only: bool
    english_ok_only: bool
    tech_only: bool
    limit: int
    # Runtime
    plan: str
    candidates: list[dict[str, Any]]
    scored: list[dict[str, Any]]
    explanations: list[dict[str, Any]]
    critic_ok: bool
    critic_notes: str
    llm_calls: int
    handoffs: int
    errors: list[str]
    hitl: bool
    method: str
    cost_summary: dict[str, Any]
    pii_redacted: dict[str, int]
    explainer_retries: int


MAX_LLM_CALLS = 4
MAX_HANDOFFS = 6
HITL_CONFIDENCE = 0.7
MAX_EXPLAINER_RETRIES = 1

"""Listwise LLM rerank over hybrid retrieval candidates."""

from __future__ import annotations

import json
import os
from typing import Any


def listwise_rerank(
    *,
    query: str,
    resume: str,
    dream_role: str,
    jobs: list[dict[str, Any]],
    router: Any | None = None,
    top_n: int = 10,
) -> list[dict[str, Any]]:
    """Rerank up to top_n jobs with one LLM call; returns full list with scores updated.

    Falls back to input order on failure / when AI disabled / MATCH_RERANK=false.
    """
    if not jobs:
        return jobs
    if os.environ.get("MATCH_RERANK", "true").lower() in {"0", "false", "no"}:
        return jobs
    if os.environ.get("AI_ENABLED", "true").lower() in {"0", "false", "no"}:
        return jobs

    try:
        from ai_gateway.providers.base import validate_json_response
        from ai_gateway.router import ModelRouter

        router = router or ModelRouter()
        slice_jobs = jobs[: min(top_n, len(jobs))]
        prompt = json.dumps(
            {
                "dream_role": dream_role,
                "query": query[:500],
                "resume_excerpt": (resume or "")[:1000],
                "jobs": [
                    {
                        "job_id": j.get("job_id"),
                        "title": j.get("title"),
                        "canonical_title_en": j.get("canonical_title_en"),
                        "company": j.get("company"),
                        "location": j.get("location"),
                        "tags": j.get("tags"),
                        "ai_skills": j.get("ai_skills"),
                        "ai_summary": j.get("ai_summary"),
                        "description": str(j.get("description") or "")[:350],
                    }
                    for j in slice_jobs
                ],
            },
            ensure_ascii=False,
        )
        system = (
            "Rerank jobs for fit. Return JSON "
            '{"scores":[{"job_id":"...","score":0.0,"note":"..."}]} only. '
            "Scores 0-1. Prefer early-career data/AI fit and skill overlap."
        )
        resp = router.complete(
            "rerank",
            prompt,
            system=system,
            json_mode=True,
            allow_local_fallback=False,
            prompt_version="rerank@v1",
        )
        ok, parsed = validate_json_response(resp.text)
        if not ok or not isinstance(parsed, dict):
            return jobs
        by_id = {
            str(s.get("job_id")): s
            for s in (parsed.get("scores") or [])
            if isinstance(s, dict) and s.get("job_id") is not None
        }
        if not by_id:
            return jobs
        rescored = []
        for job in slice_jobs:
            item = dict(job)
            s = by_id.get(str(item.get("job_id")))
            if s and s.get("score") is not None:
                item["match_score"] = round(float(s["score"]) * 100, 2)
                item["match_method"] = "hybrid_rrf+listwise"
                item["rerank_note"] = s.get("note", "")
            rescored.append(item)
        rescored.sort(key=lambda x: float(x.get("match_score") or 0), reverse=True)
        # Append non-reranked tail preserving relative order
        tail_ids = {str(j.get("job_id")) for j in slice_jobs}
        tail = [dict(j) for j in jobs if str(j.get("job_id")) not in tail_ids]
        return rescored + tail
    except Exception:
        return jobs

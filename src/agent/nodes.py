"""Agent nodes: Supervisor, Retriever, Scorer, Explainer, Critic."""

from __future__ import annotations

import json
from typing import Any

from ai_gateway.providers.base import validate_json_response
from ai_gateway.router import ModelRouter
from api.match_service import apply_profile_filters, build_citations, load_jobs_and_index
from retrieval import rank_bm25, rank_hybrid
from security.pii import redact_pii

from agent.state import HITL_CONFIDENCE, MAX_EXPLAINER_RETRIES, MAX_LLM_CALLS, AgentState

EXPLAIN_TOP_N = 5


def _router() -> ModelRouter:
    return ModelRouter()


def _load_explain_system() -> str:
    try:
        from prompts.registry import load_prompt

        return load_prompt("explain", "v2").text
    except Exception:
        return (
            'Write one short JSON object {"job_id":"...","reason":"...","evidence":"..."} '
            "using only the provided job text. evidence must be a verbatim substring. "
            "job_id must match."
        )


def _evidence_in_job(evidence: str, job: dict[str, Any]) -> bool:
    """True when evidence is a non-trivial substring of title/description/tags."""
    ev = (evidence or "").strip()
    if len(ev) < 8:
        return False
    hay = " ".join(
        [
            str(job.get("title") or ""),
            str(job.get("description") or ""),
            str(job.get("tags") or ""),
            str(job.get("ai_summary") or ""),
            str(job.get("ai_skills") or ""),
            str(job.get("ai_evidence_visa") or ""),
        ]
    )
    return ev.lower() in hay.lower()


def supervisor_node(state: AgentState) -> AgentState:
    """Plan filters and budgets — prefer zero LLM on the fast path."""
    errors = list(state.get("errors") or [])
    plan = (
        f"Retrieve tech jobs for '{state.get('dream_role','')}' in '{state.get('location','')}' "
        f"with visa={state.get('visa_status') or 'n/a'}; score top-k; explain with citations; critic-check."
    )
    return {
        **state,
        "plan": plan,
        "llm_calls": int(state.get("llm_calls") or 0),
        "handoffs": int(state.get("handoffs") or 0) + 1,
        "errors": errors,
        "method": "multi_agent",
    }


def retriever_node(state: AgentState) -> AgentState:
    """BM25 + dense hybrid — no LLM."""
    redacted = redact_pii(str(state.get("resume") or ""))
    try:
        from api.seeker_profile import extract_profile_rules
    except ImportError:
        from seeker_profile import extract_profile_rules

    profile = extract_profile_rules(
        redacted.text,
        str(state.get("dream_role") or ""),
        str(state.get("location") or ""),
        visa_status=str(state.get("visa_status") or ""),
        german_level=str(state.get("german_level") or ""),
    )
    jobs, index = load_jobs_and_index()
    filtered = apply_profile_filters(
        jobs,
        visa_status=str(state.get("visa_status") or ""),
        german_level=str(state.get("german_level") or profile.german_level or ""),
        entry_level_only=bool(state.get("entry_level_only", True)),
        english_ok_only=bool(state.get("english_ok_only")),
        tech_only=bool(state.get("tech_only", False)),
        data_ai_only=bool(state.get("data_ai_only", True)),
        audience_only=bool(state.get("audience_only", True)),
    )
    location = str(state.get("location") or "")
    if location:
        loc = location.lower()
        loc_hits = [
            j
            for j in filtered
            if loc in str(j.get("location", "")).lower()
            or ("remote" in loc and "remote" in str(j.get("location", "")).lower())
        ]
        if loc_hits:
            filtered = loc_hits

    query = profile.query_text or f"{state.get('dream_role','')} {redacted.text} {location}".strip()
    limit = int(state.get("limit") or 15)
    vectors = {e["job_id"]: e["vector"] for e in index if e.get("job_id") and e.get("vector")}
    router = _router()
    if vectors:
        ranked = rank_hybrid(
            query,
            filtered,
            vectors_by_id=vectors,
            embed_fn=lambda t, _r=router: _r.embed("embed", t).vector,
            top_k=min(max(limit * 2, 20), 50),
        )
    else:
        ranked = rank_bm25(query, filtered, top_k=min(max(limit * 2, 20), 50))

    return {
        **state,
        "candidates": ranked,
        "resume_redacted": redacted.text,
        "seeker_profile": profile.to_dict(),
        "pii_redacted": redacted.redacted_counts,
        "handoffs": int(state.get("handoffs") or 0) + 1,
        "cost_summary": router.cost_logger.summary(),
    }


def scorer_node(state: AgentState) -> AgentState:
    """Listwise LLM score using redacted resume + job summaries; falls back to retrieval scores."""
    candidates = list(state.get("candidates") or [])
    llm_calls = int(state.get("llm_calls") or 0)
    scored = []
    resume_excerpt = str(state.get("resume_redacted") or state.get("resume") or "")[:1200]

    for job in candidates[: int(state.get("limit") or 15)]:
        item = dict(job)
        raw = float(item.get("match_score") or 0)
        conf = raw if raw <= 1 else min(1.0, raw / 100.0)
        item["agent_score"] = round(conf, 4)
        item["ai_confidence"] = round(conf, 4)
        scored.append(item)

    if llm_calls < MAX_LLM_CALLS and scored:
        router = _router()
        top = scored[: min(10, len(scored))]
        prompt = json.dumps(
            {
                "dream_role": state.get("dream_role"),
                "resume_excerpt": resume_excerpt,
                "jobs": [
                    {
                        "job_id": j.get("job_id"),
                        "title": j.get("title"),
                        "company": j.get("company"),
                        "location": j.get("location"),
                        "tags": j.get("tags"),
                        "ai_skills": j.get("ai_skills"),
                        "ai_summary": j.get("ai_summary"),
                        "description": str(j.get("description") or "")[:400],
                    }
                    for j in top
                ],
            },
            ensure_ascii=False,
        )
        system = (
            "Score each job 0-1 for fit against the resume and dream role. "
            "Return JSON "
            '{"scores":[{"job_id":"...","score":0.0,"note":"..."}]} only. '
            "Use resume skills and seniority; do not invent job_ids."
        )
        try:
            resp = router.complete(
                "rerank",
                prompt,
                system=system,
                json_mode=True,
                task="rerank",
                allow_local_fallback=False,
                prompt_version="rerank@v1",
            )
            llm_calls += 1
            ok, parsed = validate_json_response(resp.text)
            if ok and parsed and isinstance(parsed.get("scores"), list):
                by_id = {s.get("job_id"): s for s in parsed["scores"] if s.get("job_id")}
                for item in scored:
                    s = by_id.get(item.get("job_id"))
                    if s and s.get("score") is not None:
                        item["agent_score"] = float(s["score"])
                        item["ai_confidence"] = float(s["score"])
                        item["score_note"] = s.get("note", "")
            cost = router.cost_logger.summary()
        except Exception as exc:
            cost = state.get("cost_summary") or {}
            errors = list(state.get("errors") or [])
            errors.append(f"scorer: {exc}")
            return {
                **state,
                "scored": scored,
                "llm_calls": llm_calls,
                "errors": errors,
                "handoffs": int(state.get("handoffs") or 0) + 1,
            }
    else:
        cost = state.get("cost_summary") or {}

    scored.sort(key=lambda x: float(x.get("agent_score") or 0), reverse=True)
    return {
        **state,
        "scored": scored[: int(state.get("limit") or 15)],
        "llm_calls": llm_calls,
        "handoffs": int(state.get("handoffs") or 0) + 1,
        "cost_summary": cost,
    }


def explainer_node(state: AgentState) -> AgentState:
    """Attach cited reasons — deterministic citations + optional LLM polish for top N."""
    scored = list(state.get("scored") or state.get("candidates") or [])
    resume = str(state.get("resume_redacted") or state.get("resume") or "")
    dream = str(state.get("dream_role") or "")
    llm_calls = int(state.get("llm_calls") or 0)
    explanations = []
    retries = int(state.get("explainer_retries") or 0)
    if state.get("critic_ok") is False:
        retries += 1

    for job in scored:
        item = dict(job)
        cites = build_citations(item, resume, dream)
        item["citations"] = [c for c in cites if c.get("job_id") == item.get("job_id")]
        if not item["citations"]:
            item["citations"] = [
                {
                    "job_id": str(item.get("job_id") or ""),
                    "reason": "Retrieved by multi-agent pipeline.",
                    "evidence": str(item.get("title") or "")[:160],
                }
            ]
        explanations.append(item)

    # Optional LLM narratives for top EXPLAIN_TOP_N when budget remains
    polish_n = min(EXPLAIN_TOP_N, len(explanations), max(0, MAX_LLM_CALLS - llm_calls))
    if polish_n > 0:
        router = _router()
        system = _load_explain_system()
        for idx in range(polish_n):
            top = explanations[idx]
            prompt = json.dumps(
                {
                    "job_id": top.get("job_id"),
                    "title": top.get("title"),
                    "description": str(top.get("description") or "")[:800],
                    "dream_role": dream,
                    "resume_excerpt": resume[:600],
                },
                ensure_ascii=False,
            )
            try:
                resp = router.complete(
                    "explain",
                    prompt,
                    system=system,
                    json_mode=True,
                    task="explain",
                    allow_local_fallback=False,
                    prompt_version="explain@v2",
                )
                llm_calls += 1
                ok, parsed = validate_json_response(resp.text)
                if (
                    ok
                    and parsed
                    and parsed.get("job_id") == top.get("job_id")
                    and _evidence_in_job(str(parsed.get("evidence") or ""), top)
                ):
                    explanations[idx]["citations"] = [
                        {
                            "job_id": parsed["job_id"],
                            "reason": str(parsed.get("reason") or "")[:300],
                            "evidence": str(parsed.get("evidence") or "")[:200],
                        }
                    ] + explanations[idx]["citations"]
            except Exception as exc:
                errors = list(state.get("errors") or [])
                errors.append(f"explainer: {exc}")
                return {
                    **state,
                    "explanations": explanations,
                    "llm_calls": llm_calls,
                    "explainer_retries": retries,
                    "errors": errors,
                    "handoffs": int(state.get("handoffs") or 0) + 1,
                }

    return {
        **state,
        "explanations": explanations,
        "llm_calls": llm_calls,
        "explainer_retries": retries,
        "handoffs": int(state.get("handoffs") or 0) + 1,
    }


def critic_node(state: AgentState) -> AgentState:
    """Validate citations + schema; require evidence substring when present; may retry explainer."""
    explanations = list(state.get("explanations") or [])
    notes: list[str] = []
    ok = True
    for job in explanations:
        jid = job.get("job_id")
        cites = job.get("citations") or []
        if not cites:
            ok = False
            notes.append(f"{jid}: missing citations")
            continue
        for c in cites:
            if c.get("job_id") != jid:
                ok = False
                notes.append(f"{jid}: citation job_id mismatch")
            if not c.get("reason"):
                ok = False
                notes.append(f"{jid}: empty reason")
            evidence = str(c.get("evidence") or "")
            # Verbatim-quote check when the job has enough text to search.
            job_text_len = len(
                f"{job.get('title') or ''}{job.get('description') or ''}{job.get('tags') or ''}"
            )
            if (
                evidence
                and len(evidence) >= 12
                and job_text_len >= 20
                and not _evidence_in_job(evidence, job)
            ):
                ok = False
                notes.append(f"{jid}: evidence not found in job text")

    avg_conf = 0.0
    if explanations:
        avg_conf = sum(float(j.get("ai_confidence") or j.get("agent_score") or 0.5) for j in explanations) / len(
            explanations
        )
    hitl = (not ok) or avg_conf < HITL_CONFIDENCE

    return {
        **state,
        "critic_ok": ok,
        "critic_notes": "; ".join(notes),
        "hitl": hitl,
        "handoffs": int(state.get("handoffs") or 0) + 1,
        "explanations": explanations,
    }


def should_retry_explainer(state: AgentState) -> str:
    """LangGraph conditional: one critic→explainer retry max."""
    if state.get("critic_ok"):
        return "end"
    retries = int(state.get("explainer_retries") or 0)
    if retries < MAX_EXPLAINER_RETRIES and int(state.get("handoffs") or 0) < 6:
        return "explain"
    return "end"

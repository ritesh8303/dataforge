"""Ingest-review agent: resolve ambiguous audience-gate rows (HITL automation).

Rules-first; optional LLM via ModelRouter for remaining ambiguous cases.
Decisions are persisted and applied by the audience gate / Gold generator.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from enrichment.ingest_review import DEFAULT_LOCAL as DEFAULT_QUEUE
from enrichment.rules_de_en import PRODUCT_DATA_AI_FIELDS, PRODUCT_SENIORITY
from processing.audience_gate import classify_for_audience

logger = logging.getLogger(__name__)

DEFAULT_DECISIONS = Path("data/hitl_ingest_review_decisions.csv")
DECISION_FIELDS = [
    "decided_at",
    "job_id",
    "decision",
    "field",
    "seniority",
    "confidence",
    "method",
    "reason",
    "title",
    "company",
    "location",
    "source",
]

MAX_LLM_DEFAULT = int(os.environ.get("INGEST_REVIEW_MAX_LLM", "40"))


def _norm(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip().lower()
    if s in {"nan", "<na>", "none", "null"}:
        return ""
    return s


def load_queue(path: str | Path | None = None) -> list[dict[str, Any]]:
    queue_path = Path(path or os.environ.get("INGEST_REVIEW_LOCAL_PATH") or DEFAULT_QUEUE)
    if not queue_path.exists():
        return []
    with queue_path.open(encoding="utf-8-sig", newline="") as fh:
        rows = list(csv.DictReader(fh))
    # Deduplicate by job_id keeping latest queued_at
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        jid = str(row.get("job_id") or "").strip()
        if not jid:
            continue
        prev = by_id.get(jid)
        if prev is None or str(row.get("queued_at") or "") >= str(prev.get("queued_at") or ""):
            by_id[jid] = row
    return list(by_id.values())


def load_decisions(path: str | Path | None = None) -> dict[str, dict[str, Any]]:
    """Return job_id -> decision row (accept|reject)."""
    dest = Path(path or os.environ.get("INGEST_REVIEW_DECISIONS_PATH") or DEFAULT_DECISIONS)
    if not dest.exists():
        return {}
    out: dict[str, dict[str, Any]] = {}
    with dest.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            jid = str(row.get("job_id") or "").strip()
            decision = _norm(row.get("decision"))
            if jid and decision in {"accept", "reject"}:
                out[jid] = row
    return out


def save_decisions(rows: list[dict[str, Any]], path: str | Path | None = None) -> str:
    dest = Path(path or os.environ.get("INGEST_REVIEW_DECISIONS_PATH") or DEFAULT_DECISIONS)
    dest.parent.mkdir(parents=True, exist_ok=True)
    existing = load_decisions(dest)
    for row in rows:
        jid = str(row.get("job_id") or "").strip()
        if jid:
            existing[jid] = row
    with dest.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=DECISION_FIELDS)
        writer.writeheader()
        for jid in sorted(existing):
            writer.writerow({k: existing[jid].get(k, "") for k in DECISION_FIELDS})
    return str(dest)


def _job_from_queue_row(row: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    raw = row.get("payload_json") or ""
    if raw:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {}
    return {
        "job_id": row.get("job_id") or payload.get("job_id") or "",
        "title": row.get("title") or payload.get("title") or "",
        "company": row.get("company") or "",
        "location": row.get("location") or "",
        "source": row.get("source") or "",
        "description": row.get("description") or payload.get("description") or "",
        "tags": row.get("tags") or "",
        "field": row.get("field") or "",
        "seniority": row.get("seniority") or "",
        "url": payload.get("url") or payload.get("job_url") or "",
        "job_url": payload.get("job_url") or payload.get("url") or "",
    }


def _rules_decide(job: dict[str, Any]) -> dict[str, Any] | None:
    """Return a decision dict if rules are confident; else None (needs LLM/human)."""
    decision = classify_for_audience(job, apply_overrides=False)
    if decision.get("audience_accept") and not decision.get("audience_uncertain"):
        return {
            "decision": "accept",
            "field": decision.get("field") or "",
            "seniority": decision.get("seniority") or "",
            "confidence": 0.9,
            "method": "rules",
            "reason": "rules_accept",
        }
    reasons = list(decision.get("audience_reject_reasons") or [])
    # Hard reject without ambiguity → agent can close as reject
    if reasons and "ambiguous_needs_review" not in reasons and not decision.get("audience_uncertain"):
        return {
            "decision": "reject",
            "field": decision.get("field") or "",
            "seniority": decision.get("seniority") or "",
            "confidence": 0.85,
            "method": "rules",
            "reason": ",".join(reasons) or "rules_reject",
        }
    return None


def _llm_decide(job: dict[str, Any], *, router: Any) -> dict[str, Any] | None:
    system = (
        "You are DataForge's ingest-review agent. Decide if a job belongs on the "
        "EU early-career data/AI product board. "
        "Accept ONLY if ALL are true: (1) EU location, (2) data/AI related field, "
        "(3) early career: internship, working_student, thesis, trainee_graduate, or junior. "
        "Reject mid/senior, non-data, or non-EU. "
        'Return JSON: {"decision":"accept"|"reject","field":"...","seniority":"...",'
        '"confidence":0-1,"reason":"short"}.'
    )
    prompt = json.dumps(
        {
            "title": job.get("title"),
            "company": job.get("company"),
            "location": job.get("location"),
            "source": job.get("source"),
            "description": str(job.get("description") or "")[:800],
            "allowed_fields": sorted(PRODUCT_DATA_AI_FIELDS),
            "allowed_seniorities": sorted(PRODUCT_SENIORITY),
        },
        ensure_ascii=False,
    )
    try:
        resp = router.complete("enrich", prompt, system=system, json_mode=True)
        from ai_gateway.providers.base import validate_json_response

        ok, parsed = validate_json_response(resp.text)
        if not ok or not isinstance(parsed, dict):
            return None
        decision = _norm(parsed.get("decision"))
        if decision not in {"accept", "reject"}:
            return None
        field = _norm(parsed.get("field"))
        seniority = _norm(parsed.get("seniority"))
        if decision == "accept":
            if field and field not in PRODUCT_DATA_AI_FIELDS:
                # Map common aliases
                if re.search(r"data|analytics|ml|ai|bi", field):
                    field = "data_analytics"
                else:
                    decision = "reject"
            if seniority and seniority not in PRODUCT_SENIORITY:
                decision = "reject"
        conf = parsed.get("confidence", 0.6)
        try:
            conf_f = float(conf)
        except (TypeError, ValueError):
            conf_f = 0.6
        return {
            "decision": decision,
            "field": field or "",
            "seniority": seniority or "",
            "confidence": round(min(1.0, max(0.0, conf_f)), 3),
            "method": "llm",
            "reason": str(parsed.get("reason") or "llm")[:300],
        }
    except Exception as exc:  # pragma: no cover
        logger.warning("Ingest review LLM failed for %s: %s", job.get("job_id"), exc)
        return None


def review_row(
    row: dict[str, Any],
    *,
    use_llm: bool = True,
    router: Any | None = None,
    llm_budget: list[int] | None = None,
) -> dict[str, Any]:
    """Review one queue row → decision record."""
    job = _job_from_queue_row(row)
    now = datetime.now(timezone.utc).isoformat()
    base = {
        "decided_at": now,
        "job_id": job.get("job_id") or "",
        "title": job.get("title") or "",
        "company": job.get("company") or "",
        "location": job.get("location") or "",
        "source": job.get("source") or "",
    }

    rules = _rules_decide(job)
    if rules:
        return {**base, **rules}

    if use_llm and router is not None and (llm_budget is None or llm_budget[0] > 0):
        llm = _llm_decide(job, router=router)
        if llm_budget is not None:
            llm_budget[0] -= 1
        if llm:
            return {**base, **llm}

    # Still ambiguous — leave as reject-for-now with explicit method so humans can override
    return {
        **base,
        "decision": "reject",
        "field": row.get("field") or "",
        "seniority": row.get("seniority") or "",
        "confidence": 0.4,
        "method": "deferred",
        "reason": "still_ambiguous_needs_human",
    }


def run_ingest_review_agent(
    *,
    queue_path: str | Path | None = None,
    decisions_path: str | Path | None = None,
    use_llm: bool | None = None,
    max_llm: int | None = None,
    limit: int | None = None,
    skip_decided: bool = True,
) -> dict[str, Any]:
    """Process the ingest HITL queue and persist accept/reject decisions."""
    if use_llm is None:
        use_llm = os.environ.get("INGEST_REVIEW_USE_LLM", "true").lower() != "false"
    max_llm = MAX_LLM_DEFAULT if max_llm is None else max_llm

    queue = load_queue(queue_path)
    existing = load_decisions(decisions_path) if skip_decided else {}
    pending = [r for r in queue if str(r.get("job_id") or "") not in existing]
    if limit is not None:
        pending = pending[: max(0, int(limit))]

    router = None
    if use_llm and pending:
        try:
            from ai_gateway.router import ModelRouter

            router = ModelRouter()
        except Exception as exc:  # pragma: no cover
            logger.warning("ModelRouter unavailable, rules-only: %s", exc)
            use_llm = False

    budget = [max_llm]
    decisions: list[dict[str, Any]] = []
    for row in pending:
        decisions.append(
            review_row(row, use_llm=use_llm, router=router, llm_budget=budget)
        )

    dest = save_decisions(decisions, decisions_path) if decisions else str(
        Path(decisions_path or os.environ.get("INGEST_REVIEW_DECISIONS_PATH") or DEFAULT_DECISIONS)
    )
    counts = {"accept": 0, "reject": 0}
    methods: dict[str, int] = {}
    for d in decisions:
        counts[d.get("decision") or "reject"] = counts.get(d.get("decision") or "reject", 0) + 1
        methods[d.get("method") or "?"] = methods.get(d.get("method") or "?", 0) + 1

    return {
        "queue_size": len(queue),
        "pending": len(pending),
        "decided": len(decisions),
        "counts": counts,
        "methods": methods,
        "decisions_path": dest,
        "llm_used": max_llm - budget[0],
        "method": "ingest_review_agent",
    }

"""Embedding index utilities for semantic job matching.

Supports incremental rebuilds keyed by content hash so unchanged jobs are not
re-embedded (cost control for nightly enrichment).
"""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any

from ai_gateway.router import ModelRouter


def cosine_similarity(a: list[float], b: list[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1.0
    nb = math.sqrt(sum(y * y for y in b)) or 1.0
    return dot / (na * nb)


def job_text(job: dict[str, Any]) -> str:
    parts = [
        str(job.get("title", "")),
        str(job.get("company", "")),
        str(job.get("tags", "")),
        str(job.get("description", ""))[:1500],
        str(job.get("ai_summary", "")),
        str(job.get("ai_skills", "")),
    ]
    return " ".join(p for p in parts if p).strip()


def content_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()[:16]


def job_content_hash(job: dict[str, Any]) -> str:
    return content_hash(job_text(job))


def build_embedding_index(
    jobs: list[dict],
    router: ModelRouter | None = None,
    *,
    existing: list[dict] | None = None,
    force: bool = False,
) -> list[dict]:
    """Build list of {job_id, vector, model, provider, content_hash} for jobs.

    When ``existing`` is provided and ``force`` is False, reuse vectors whose
    content_hash still matches — only embed new or changed jobs.
    """
    router = router or ModelRouter()
    by_id: dict[str, dict] = {}
    if existing and not force:
        for entry in existing:
            jid = str(entry.get("job_id") or "")
            if jid and entry.get("vector"):
                by_id[jid] = dict(entry)

    index: list[dict] = []
    for job in jobs:
        jid = str(job.get("job_id") or job.get("id") or "")
        if not jid:
            continue
        text = job_text(job)
        if not text:
            continue
        digest = content_hash(text)
        prev = by_id.get(jid)
        if (
            prev
            and not force
            and prev.get("content_hash") == digest
            and prev.get("vector")
        ):
            index.append(
                {
                    "job_id": jid,
                    "vector": prev["vector"],
                    "model": prev.get("model", ""),
                    "provider": prev.get("provider", ""),
                    "content_hash": digest,
                }
            )
            continue
        emb = router.embed("embed", text)
        index.append(
            {
                "job_id": jid,
                "vector": emb.vector,
                "model": emb.model_id,
                "provider": emb.provider,
                "content_hash": digest,
            }
        )
    return index


def rank_by_embedding(
    query_text: str,
    jobs: list[dict],
    index: list[dict],
    top_k: int = 20,
    router: ModelRouter | None = None,
) -> list[dict]:
    router = router or ModelRouter()
    query_vec = router.embed("embed", query_text).vector
    vec_by_id = {item["job_id"]: item["vector"] for item in index}
    scored = []
    for job in jobs:
        jid = job.get("job_id") or job.get("id")
        vec = vec_by_id.get(jid)
        if not vec:
            continue
        score = cosine_similarity(query_vec, vec)
        scored.append({**job, "match_score": round(score * 100, 2), "match_method": "embedding"})
    scored.sort(key=lambda x: x["match_score"], reverse=True)
    return scored[:top_k]


def index_to_json(index: list[dict]) -> str:
    return json.dumps({"version": 2, "entries": index})


def index_from_json(raw: str | bytes) -> list[dict]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    data = json.loads(raw)
    return data.get("entries", data if isinstance(data, list) else [])

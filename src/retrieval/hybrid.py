"""Dense ranking and Reciprocal Rank Fusion hybrid retrieval."""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Callable, Sequence

from retrieval.bm25 import job_document, rank_bm25


def cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a)) or 1.0
    nb = math.sqrt(sum(y * y for y in b)) or 1.0
    return dot / (na * nb)


def reciprocal_rank_fusion(
    ranked_lists: Sequence[Sequence[str]],
    k: int = 60,
) -> list[tuple[str, float]]:
    scores: dict[str, float] = defaultdict(float)
    for ranked in ranked_lists:
        for rank, doc_id in enumerate(ranked, start=1):
            if not doc_id:
                continue
            scores[doc_id] += 1.0 / (k + rank)
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)


def rank_dense(
    query: str,
    jobs: Sequence[dict[str, Any]],
    vectors_by_id: dict[str, Sequence[float]],
    embed_fn: Callable[[str], Sequence[float]],
    top_k: int = 20,
) -> list[dict[str, Any]]:
    qvec = embed_fn(query)
    scored = []
    for job in jobs:
        jid = str(job.get("job_id") or job.get("id") or "")
        vec = vectors_by_id.get(jid)
        if not vec:
            continue
        score = cosine_similarity(qvec, vec)
        scored.append({**job, "match_score": round(score * 100, 2), "match_method": "dense"})
    scored.sort(key=lambda x: x["match_score"], reverse=True)
    return scored[:top_k]


def rank_hybrid(
    query: str,
    jobs: Sequence[dict[str, Any]],
    vectors_by_id: dict[str, Sequence[float]] | None = None,
    embed_fn: Callable[[str], Sequence[float]] | None = None,
    top_k: int = 20,
    rrf_k: int = 60,
) -> list[dict[str, Any]]:
    bm25 = rank_bm25(query, jobs, top_k=max(top_k * 2, 20))
    lists: list[list[str]] = [[str(j.get("job_id")) for j in bm25]]

    dense: list[dict[str, Any]] = []
    if vectors_by_id and embed_fn:
        dense = rank_dense(query, jobs, vectors_by_id, embed_fn, top_k=max(top_k * 2, 20))
        lists.append([str(j.get("job_id")) for j in dense])

    fused = reciprocal_rank_fusion(lists, k=rrf_k)
    by_id = {str(j.get("job_id")): j for j in jobs}
    score_bm25 = {str(j.get("job_id")): j["match_score"] for j in bm25}
    score_dense = {str(j.get("job_id")): j["match_score"] for j in dense}

    out = []
    for jid, rrf_score in fused[:top_k]:
        job = by_id.get(jid)
        if not job:
            continue
        out.append(
            {
                **job,
                "match_score": round(rrf_score, 6),
                "match_method": "hybrid_rrf",
                "bm25_score": score_bm25.get(jid),
                "dense_score": score_dense.get(jid),
            }
        )
    return out


__all__ = [
    "cosine_similarity",
    "job_document",
    "rank_bm25",
    "rank_dense",
    "rank_hybrid",
    "reciprocal_rank_fusion",
]

"""Evaluation utilities for thesis experiments."""

from __future__ import annotations

import math
from typing import Sequence


def precision_at_k(relevant: set[str], ranked: Sequence[str], k: int) -> float:
    if k <= 0:
        return 0.0
    top = ranked[:k]
    if not top:
        return 0.0
    return sum(1 for jid in top if jid in relevant) / len(top)


def dcg_at_k(relevant: set[str], ranked: Sequence[str], k: int) -> float:
    score = 0.0
    for i, jid in enumerate(ranked[:k], start=1):
        rel = 1.0 if jid in relevant else 0.0
        score += rel / math.log2(i + 1)
    return score


def ndcg_at_k(relevant: set[str], ranked: Sequence[str], k: int) -> float:
    ideal = dcg_at_k(relevant, list(relevant), k)
    if ideal == 0:
        return 0.0
    return dcg_at_k(relevant, ranked, k) / ideal


def skill_f1(predicted: set[str], gold: set[str]) -> dict[str, float]:
    if not predicted and not gold:
        return {"precision": 1.0, "recall": 1.0, "f1": 1.0}
    tp = len(predicted & gold)
    precision = tp / len(predicted) if predicted else 0.0
    recall = tp / len(gold) if gold else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"precision": precision, "recall": recall, "f1": f1}

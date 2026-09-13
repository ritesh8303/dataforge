"""Evaluation utilities for thesis experiments."""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Sequence


def precision_at_k(relevant: set[str], ranked: Sequence[str], k: int) -> float:
    if k <= 0:
        return 0.0
    top = ranked[:k]
    if not top:
        return 0.0
    return sum(1 for jid in top if jid in relevant) / len(top)


def recall_at_k(relevant: set[str], ranked: Sequence[str], k: int) -> float:
    if not relevant:
        return 0.0
    top = set(ranked[:k])
    return len(top & relevant) / len(relevant)


def mean_reciprocal_rank(relevant: set[str], ranked: Sequence[str]) -> float:
    for i, jid in enumerate(ranked, start=1):
        if jid in relevant:
            return 1.0 / i
    return 0.0


def dcg_at_k(relevances: Sequence[float], k: int) -> float:
    score = 0.0
    for i, rel in enumerate(list(relevances)[:k], start=1):
        score += (2**rel - 1) / math.log2(i + 1)
    return score


def ndcg_at_k(relevant: set[str] | dict[str, float], ranked: Sequence[str], k: int) -> float:
    """Supports binary sets or graded relevance dict {job_id: grade}."""
    if isinstance(relevant, dict):
        grades = relevant
    else:
        grades = {jid: 1.0 for jid in relevant}

    actual = [float(grades.get(jid, 0.0)) for jid in ranked[:k]]
    ideal_vals = sorted(grades.values(), reverse=True)[:k]
    ideal = dcg_at_k(ideal_vals, k)
    if ideal == 0:
        return 0.0
    return dcg_at_k(actual, k) / ideal


def skill_f1(predicted: set[str], gold: set[str]) -> dict[str, float]:
    if not predicted and not gold:
        return {"precision": 1.0, "recall": 1.0, "f1": 1.0}
    tp = len(predicted & gold)
    precision = tp / len(predicted) if predicted else 0.0
    recall = tp / len(gold) if gold else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {"precision": precision, "recall": recall, "f1": f1}


def classification_report(
    y_true: Sequence[str],
    y_pred: Sequence[str],
    labels: Sequence[str] | None = None,
) -> dict[str, dict[str, float] | float]:
    """Per-class precision/recall/F1 plus macro/micro averages."""
    if len(y_true) != len(y_pred):
        raise ValueError("y_true and y_pred length mismatch")
    label_set = list(labels) if labels is not None else sorted(set(y_true) | set(y_pred))
    tp: dict[str, int] = defaultdict(int)
    fp: dict[str, int] = defaultdict(int)
    fn: dict[str, int] = defaultdict(int)
    for t, p in zip(y_true, y_pred):
        if t == p:
            tp[t] += 1
        else:
            fp[p] += 1
            fn[t] += 1

    per_class: dict[str, dict[str, float]] = {}
    for label in label_set:
        prec = tp[label] / (tp[label] + fp[label]) if (tp[label] + fp[label]) else 0.0
        rec = tp[label] / (tp[label] + fn[label]) if (tp[label] + fn[label]) else 0.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
        per_class[label] = {
            "precision": round(prec, 4),
            "recall": round(rec, 4),
            "f1": round(f1, 4),
            "support": float(tp[label] + fn[label]),
        }

    supports = [per_class[l]["support"] for l in label_set]
    if label_set:
        macro_f1 = sum(per_class[l]["f1"] for l in label_set) / len(label_set)
    else:
        macro_f1 = 0.0
    total_tp = sum(tp[l] for l in label_set)
    total = len(y_true) or 1
    micro_f1 = total_tp / total
    return {
        "per_class": per_class,
        "macro_f1": round(macro_f1, 4),
        "micro_f1": round(micro_f1, 4),
        "n": float(len(y_true)),
        "support_total": float(sum(supports)),
    }


def confusion_pairs(y_true: Sequence[str], y_pred: Sequence[str]) -> dict[str, dict[str, int]]:
    matrix: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for t, p in zip(y_true, y_pred):
        matrix[str(t)][str(p)] += 1
    return {k: dict(v) for k, v in matrix.items()}

"""Minimal BM25 Okapi over an in-memory job corpus."""

from __future__ import annotations

import math
import re
from collections import Counter
from typing import Any, Sequence

TOKEN_RE = re.compile(r"[a-z0-9äöüß+#.]{2,}", re.I)


def tokenize(text: str) -> list[str]:
    return [t.lower() for t in TOKEN_RE.findall(text or "")]


class BM25Index:
    def __init__(self, documents: Sequence[str], k1: float = 1.5, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self.doc_tokens = [tokenize(doc) for doc in documents]
        self.doc_len = [len(toks) for toks in self.doc_tokens]
        self.avgdl = (sum(self.doc_len) / len(self.doc_len)) if self.doc_len else 0.0
        self.doc_freq: Counter[str] = Counter()
        self.tf: list[Counter[str]] = []
        for toks in self.doc_tokens:
            counts = Counter(toks)
            self.tf.append(counts)
            for term in counts:
                self.doc_freq[term] += 1
        self.n = len(self.doc_tokens)

    def _idf(self, term: str) -> float:
        df = self.doc_freq.get(term, 0)
        return math.log(1 + (self.n - df + 0.5) / (df + 0.5))

    def score(self, query: str) -> list[float]:
        q_terms = tokenize(query)
        scores = [0.0] * self.n
        if not q_terms or self.n == 0 or self.avgdl == 0:
            return scores
        for i, counts in enumerate(self.tf):
            dl = self.doc_len[i]
            s = 0.0
            for term in q_terms:
                if term not in counts:
                    continue
                tf = counts[term]
                idf = self._idf(term)
                denom = tf + self.k1 * (1 - self.b + self.b * dl / self.avgdl)
                s += idf * (tf * (self.k1 + 1)) / denom
            scores[i] = s
        return scores

    def rank(self, query: str, top_k: int = 20) -> list[tuple[int, float]]:
        scored = list(enumerate(self.score(query)))
        scored.sort(key=lambda x: x[1], reverse=True)
        return [(i, s) for i, s in scored[:top_k] if s > 0]


def job_document(job: dict[str, Any]) -> str:
    parts = [
        str(job.get("title", "")),
        str(job.get("company", "")),
        str(job.get("location", "")),
        str(job.get("tags", "")),
        str(job.get("ai_skills", "")),
        str(job.get("ai_summary", "")),
        str(job.get("description", ""))[:2000],
    ]
    return " ".join(p for p in parts if p).strip()


def rank_bm25(query: str, jobs: Sequence[dict[str, Any]], top_k: int = 20) -> list[dict[str, Any]]:
    docs = [job_document(j) for j in jobs]
    index = BM25Index(docs)
    ranked = index.rank(query, top_k=top_k)
    out = []
    for idx, score in ranked:
        job = dict(jobs[idx])
        job["match_score"] = round(score, 4)
        job["match_method"] = "bm25"
        out.append(job)
    return out

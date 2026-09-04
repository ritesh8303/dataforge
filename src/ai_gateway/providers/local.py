"""Local fallback provider — no API keys, deterministic, for tests and offline mode."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter

from ai_gateway.providers.base import BaseProvider, timed_call
from ai_gateway.types import EmbeddingResponse, ProviderResponse

_TOKEN_RE = re.compile(r"[a-zA-Z0-9+#./]+")
_EMBED_DIM = 128


def _tokenize(text: str) -> list[str]:
    return [t.lower() for t in _TOKEN_RE.findall(text or "")]


def _hash_embed(text: str, dim: int = _EMBED_DIM) -> list[float]:
    """Deterministic sparse hash embedding — thesis local baseline without API deps."""
    tokens = _tokenize(text)
    if not tokens:
        return [0.0] * dim
    vec = [0.0] * dim
    for tok in tokens:
        h = int(hashlib.md5(tok.encode()).hexdigest(), 16)
        idx = h % dim
        sign = 1.0 if (h >> 1) % 2 == 0 else -1.0
        vec[idx] += sign
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


class LocalProvider(BaseProvider):
    name = "local"

    def complete(self, prompt: str, system: str = "", model: str | None = None, **kwargs) -> ProviderResponse:
        def _run():
            task = kwargs.get("task", "summarize")
            if task == "enrich" or kwargs.get("json_mode"):
                return self._enrich_json(prompt)
            return self._summarize(prompt)

        text, latency_ms = timed_call(_run)
        return ProviderResponse(
            text=text,
            model_id=model or "local-heuristic",
            provider=self.name,
            input_tokens=len(_tokenize(prompt + system)),
            output_tokens=len(_tokenize(text)),
            latency_ms=latency_ms,
        )

    def embed(self, text: str, model: str | None = None, **kwargs) -> EmbeddingResponse:
        vec, latency_ms = timed_call(_hash_embed, text)
        return EmbeddingResponse(
            vector=vec,
            model_id=model or "local-tfidf",
            provider=self.name,
            input_tokens=len(_tokenize(text)),
            latency_ms=latency_ms,
        )

    def _summarize(self, prompt: str) -> str:
        tokens = _tokenize(prompt)
        if not tokens:
            return "No content to summarize."
        top = Counter(tokens).most_common(8)
        keywords = ", ".join(w for w, _ in top)
        return f"Role summary (local): focuses on {keywords}."

    def _enrich_json(self, prompt: str) -> str:
        text = prompt.lower()
        skills = []
        for kw in [
            "python", "sql", "aws", "spark", "kafka", "docker", "kubernetes",
            "machine learning", "llm", "pytorch", "terraform", "java", "scala",
        ]:
            if kw in text:
                skills.append(kw.title() if kw != "llm" else "LLM")
        seniority = "mid"
        if any(x in text for x in ("senior", "lead", "principal", "staff")):
            seniority = "senior"
        elif any(x in text for x in ("junior", "entry", "graduate", "einsteiger")):
            seniority = "junior"
        remote_conf = 0.9 if any(x in text for x in ("remote", "home office", "hybrid")) else 0.2
        payload = {
            "skills": skills[:10],
            "seniority": seniority,
            "summary": self._summarize(prompt),
            "remote_confidence": remote_conf,
            "language": "english" if "english" in text else "unknown",
            "provider": "local",
        }
        return json.dumps(payload)

"""Lightweight language detection for job postings (DE / EN / bilingual).

No external model dependency — character + stopword heuristics suitable for
DACH job text. Good enough for Gold KPIs; swap for fastText later if needed.
"""

from __future__ import annotations

import re
from typing import Any

_DE_STOP = {
    "und", "der", "die", "das", "mit", "für", "von", "bei", "wir", "sie", "eine",
    "einen", "oder", "auch", "als", "im", "zum", "zur", "über", "sowie", "bewerbung",
    "aufgaben", "anforderungen", "kenntnisse", "erfahrung", "team",
}
_EN_STOP = {
    "the", "and", "with", "for", "you", "our", "your", "will", "are", "this",
    "that", "from", "have", "role", "team", "experience", "skills", "requirements",
    "responsibilities", "about", "work", "we",
}
_DE_CHARS = re.compile(r"[äöüßÄÖÜ]")


def detect_language(text: str = "") -> dict[str, Any]:
    """Return {language: en|de|bilingual|unknown, confidence, method}."""
    blob = (text or "").strip()
    if len(blob) < 20:
        return {"language": "unknown", "confidence": 0.0, "method": "too_short"}

    tokens = re.findall(r"[A-Za-zÄÖÜäöüß]{2,}", blob.lower())
    if not tokens:
        return {"language": "unknown", "confidence": 0.0, "method": "no_tokens"}

    de_hits = sum(1 for t in tokens if t in _DE_STOP)
    en_hits = sum(1 for t in tokens if t in _EN_STOP)
    umlaut = 1 if _DE_CHARS.search(blob) else 0
    de_score = de_hits + umlaut * 3
    en_score = en_hits
    total = de_score + en_score
    if total == 0:
        # Title-case English-ish vs German compound guess
        if umlaut:
            return {"language": "de", "confidence": 0.55, "method": "umlaut"}
        return {"language": "unknown", "confidence": 0.2, "method": "no_stopwords"}

    de_ratio = de_score / total
    en_ratio = en_score / total
    if de_ratio >= 0.35 and en_ratio >= 0.35:
        return {"language": "bilingual", "confidence": round(min(de_ratio, en_ratio) + 0.3, 3), "method": "stopwords"}
    if de_score > en_score:
        return {"language": "de", "confidence": round(0.5 + 0.5 * de_ratio, 3), "method": "stopwords"}
    if en_score > de_score:
        return {"language": "en", "confidence": round(0.5 + 0.5 * en_ratio, 3), "method": "stopwords"}
    return {"language": "bilingual", "confidence": 0.5, "method": "stopwords"}

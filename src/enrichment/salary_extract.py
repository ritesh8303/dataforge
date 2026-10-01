"""Salary extraction from free-text job postings (EU / DACH heuristics)."""

from __future__ import annotations

import re
from typing import Any

_EUR_RANGE = re.compile(
    r"(?P<min>\d{1,3}(?:[.\s]\d{3})+|\d+)"
    r"(?:\s*[,.]\s*(?P<dec>\d{1,2}))?"
    r"\s*(?:€|eur|euro)\s*(?:-|–|—|bis|to|/)\s*"
    r"(?P<max>\d{1,3}(?:[.\s]\d{3})+|\d+)"
    r"(?:\s*[,.]\s*(?P<dec2>\d{1,2}))?",
    re.I,
)
_EUR_SINGLE = re.compile(
    r"(?P<val>\d{1,3}(?:[.\s]\d{3})+|\d{4,6})\s*(?:€|eur|euro)\s*"
    r"(?P<unit>(?:pro\s*)?(?:jahr|year|annum|monat|month|stunde|hour|/h|/m|/a))?",
    re.I,
)
_UNIT_YEAR = re.compile(r"jahr|year|annum|/a|p\.?a\.?", re.I)
_UNIT_MONTH = re.compile(r"monat|month|/m", re.I)
_UNIT_HOUR = re.compile(r"stunde|hour|/h", re.I)


def _to_float(num: str, dec: str | None = None) -> float | None:
    raw = (num or "").replace(" ", "").replace(".", "")
    if not raw.isdigit():
        return None
    val = float(raw)
    if dec and dec.isdigit():
        val += float(dec) / (10 ** len(dec))
    return val


def _unit_from_text(blob: str) -> str | None:
    if _UNIT_HOUR.search(blob):
        return "eur_hour"
    if _UNIT_MONTH.search(blob):
        return "eur_month"
    if _UNIT_YEAR.search(blob):
        return "eur_year"
    return None


def extract_salary(text: str = "") -> dict[str, Any]:
    """Return salary_min, salary_max, salary_unit, evidence."""
    blob = text or ""
    if not blob.strip():
        return {"salary_min": None, "salary_max": None, "salary_unit": None, "evidence": ""}

    m = _EUR_RANGE.search(blob)
    if m:
        lo = _to_float(m.group("min"), m.group("dec"))
        hi = _to_float(m.group("max"), m.group("dec2"))
        unit = _unit_from_text(m.group(0)) or _unit_from_text(blob[m.start() : m.end() + 40])
        # Heuristic: large numbers without unit → yearly
        if unit is None and lo and lo >= 10000:
            unit = "eur_year"
        elif unit is None and lo and lo < 100:
            unit = "eur_hour"
        elif unit is None:
            unit = "eur_month"
        return {
            "salary_min": lo,
            "salary_max": hi,
            "salary_unit": unit,
            "evidence": m.group(0)[:120],
        }

    m2 = _EUR_SINGLE.search(blob)
    if m2:
        val = _to_float(m2.group("val"))
        unit = _unit_from_text(m2.group("unit") or "") or _unit_from_text(m2.group(0))
        if unit is None and val and val >= 10000:
            unit = "eur_year"
        elif unit is None and val and val < 100:
            unit = "eur_hour"
        elif unit is None:
            unit = "eur_month"
        return {
            "salary_min": val,
            "salary_max": val,
            "salary_unit": unit,
            "evidence": m2.group(0)[:120],
        }
    return {"salary_min": None, "salary_max": None, "salary_unit": None, "evidence": ""}

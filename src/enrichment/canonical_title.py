"""Canonical bilingual job titles for retrieval / UI (DE ↔ EN)."""

from __future__ import annotations

import re
from typing import Any

# Ordered: first match wins. Pattern → English canonical title stem.
_TITLE_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"werkstudent|working\s*student|studentische[rn]?\s+mitarbeit", re.I), "Working Student"),
    (re.compile(r"praktikum|internship|intern\b", re.I), "Internship"),
    (re.compile(r"masterarbeit|bachelorarbeit|abschlussarbeit|thesis", re.I), "Thesis"),
    (re.compile(r"trainee|absolvent|graduate\s*program|berufseinsteiger", re.I), "Graduate / Trainee"),
    (re.compile(r"data\s*engineer|dateningenieur", re.I), "Data Engineer"),
    (re.compile(r"data\s*scientist|datenwissenschaftler", re.I), "Data Scientist"),
    (re.compile(r"data\s*analyst|datenanalyst", re.I), "Data Analyst"),
    (re.compile(r"machine\s*learning|ml\s*engineer|ki\s*ingenieur", re.I), "Machine Learning Engineer"),
    (re.compile(r"analytics\s*engineer", re.I), "Analytics Engineer"),
    (re.compile(r"business\s*intelligence|\bbi\b", re.I), "Business Intelligence"),
    (re.compile(r"mlops|ml\s*ops", re.I), "MLOps Engineer"),
    (re.compile(r"devops|site\s*reliability|sre", re.I), "DevOps / SRE"),
    (re.compile(r"software\s*engineer|softwareentwickler", re.I), "Software Engineer"),
]


def canonical_title_en(title: str = "", field: str = "", seniority: str = "") -> str:
    """Produce a short English canonical title for matching / SEO."""
    raw = (title or "").strip()
    if not raw:
        return ""
    role = ""
    level = ""
    for pattern, label in _TITLE_RULES:
        if pattern.search(raw):
            if label in {"Working Student", "Internship", "Thesis", "Graduate / Trainee"}:
                level = label
            elif not role:
                role = label
    if not role:
        field_map = {
            "data_engineering": "Data Engineer",
            "ai_ml_data_science": "Data / ML",
            "data_analytics": "Data Analyst",
            "business_intelligence": "Business Intelligence",
            "cloud_devops": "Cloud / DevOps",
        }
        role = field_map.get(str(field or "").lower(), "Data / AI Role")
    if not level:
        sen = str(seniority or "").lower()
        level_map = {
            "working_student": "Working Student",
            "internship": "Internship",
            "thesis": "Thesis",
            "trainee_graduate": "Graduate / Trainee",
            "fresher": "Entry-Level",
            "junior": "Junior",
        }
        level = level_map.get(sen, "")
    if level and role:
        if level in role:
            return role
        return f"{level} {role}"
    return level or role or raw[:80]


def attach_canonical_title(job: dict[str, Any]) -> dict[str, Any]:
    out = dict(job)
    out["canonical_title_en"] = canonical_title_en(
        str(job.get("title") or ""),
        str(job.get("ai_field") or job.get("field") or ""),
        str(job.get("ai_seniority") or job.get("seniority") or ""),
    )
    return out

"""Structured seeker profile extracted from resume / dream role (Tier 1).

Used by Match to build better retrieval queries and explain skill gaps.
Rules-first; optional LLM when AI is enabled and budget remains.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any

from security.pii import redact_pii

_SKILL_TERMS = [
    "python",
    "sql",
    "spark",
    "airflow",
    "dbt",
    "aws",
    "azure",
    "gcp",
    "kafka",
    "pandas",
    "pytorch",
    "tensorflow",
    "docker",
    "kubernetes",
    "terraform",
    "snowflake",
    "databricks",
    "mlflow",
    "power bi",
    "tableau",
    "java",
    "scala",
    "r ",
    "nlp",
    "llm",
    "machine learning",
    "deep learning",
]

_CITY_HINTS = [
    "berlin",
    "munich",
    "münchen",
    "hamburg",
    "frankfurt",
    "cologne",
    "köln",
    "stuttgart",
    "amsterdam",
    "paris",
    "dublin",
    "vienna",
    "wien",
    "remote",
]


@dataclass
class SeekerProfile:
    skills: list[str] = field(default_factory=list)
    dream_role: str = ""
    cities: list[str] = field(default_factory=list)
    languages: list[str] = field(default_factory=list)
    german_level: str = ""
    visa_status: str = ""
    seniority_target: str = "fresher"
    hours_per_week: int | None = None
    summary: str = ""
    query_text: str = ""
    source: str = "rules"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _skills_from_text(text: str) -> list[str]:
    t = f" {text.lower()} "
    found = []
    for s in _SKILL_TERMS:
        token = s.strip()
        if token and token in t:
            found.append(token)
    return sorted(set(found))


def _cities_from_text(text: str) -> list[str]:
    t = text.lower()
    return sorted({c for c in _CITY_HINTS if c in t})


def extract_profile_rules(
    resume: str = "",
    dream_role: str = "",
    location: str = "",
    *,
    visa_status: str = "",
    german_level: str = "",
) -> SeekerProfile:
    redacted = redact_pii(resume or "")
    blob = f"{dream_role} {redacted.text} {location}".strip()
    skills = _skills_from_text(blob)
    cities = _cities_from_text(blob)
    if location and location.lower() not in cities:
        cities = sorted(set(cities + [location.strip().lower()]))

    seniority = "fresher"
    low = blob.lower()
    if any(x in low for x in ("werkstudent", "working student")):
        seniority = "working_student"
    elif any(x in low for x in ("internship", "praktikum", "intern")):
        seniority = "internship"
    elif any(x in low for x in ("thesis", "masterarbeit", "bachelorarbeit")):
        seniority = "thesis"
    elif any(x in low for x in ("junior",)):
        seniority = "junior"

    langs: list[str] = []
    if re.search(r"\benglish\b|\benglisch\b", low):
        langs.append("en")
    if re.search(r"\bgerman\b|\bdeutsch\b", low):
        langs.append("de")

    cefr = german_level.upper().strip()
    if not cefr:
        m = re.search(r"\b([ABC][12])\b", blob, flags=re.I)
        if m:
            cefr = m.group(1).upper()

    query_parts = [
        dream_role,
        " ".join(skills),
        " ".join(cities),
        redacted.text[:800],
    ]
    query = " ".join(p for p in query_parts if p).strip()
    summary = (
        f"Target: {dream_role or seniority}. Skills: {', '.join(skills[:8]) or 'n/a'}."
        if dream_role or skills
        else "Profile extracted from resume."
    )
    return SeekerProfile(
        skills=skills,
        dream_role=dream_role,
        cities=cities,
        languages=langs,
        german_level=cefr,
        visa_status=visa_status,
        seniority_target=seniority,
        summary=summary,
        query_text=query,
        source="rules",
    )


def extract_profile(
    resume: str = "",
    dream_role: str = "",
    location: str = "",
    *,
    visa_status: str = "",
    german_level: str = "",
    use_llm: bool = False,
    router: Any | None = None,
) -> SeekerProfile:
    """Rules-first profile; optional LLM refinement when use_llm=True."""
    base = extract_profile_rules(
        resume,
        dream_role,
        location,
        visa_status=visa_status,
        german_level=german_level,
    )
    if not use_llm:
        return base
    try:
        from ai_gateway.providers.base import validate_json_response
        from ai_gateway.router import ModelRouter

        router = router or ModelRouter()
        redacted = redact_pii(resume or "")
        system = (
            "Extract a job-seeker profile for EU early-career data/AI matching. "
            "Return JSON only: "
            '{"skills":[],"cities":[],"languages":[],"german_level":"",'
            '"seniority_target":"fresher|junior|internship|working_student|thesis",'
            '"summary":"..."} . Use only evidence from the resume.'
        )
        prompt = json.dumps(
            {
                "dream_role": dream_role,
                "location": location,
                "resume": redacted.text[:2000],
            },
            ensure_ascii=False,
        )
        resp = router.complete(
            "summarize",
            prompt,
            system=system,
            json_mode=True,
            allow_local_fallback=True,
            prompt_version="profile@v1",
        )
        ok, parsed = validate_json_response(resp.text)
        if not ok or not isinstance(parsed, dict):
            return base
        skills = parsed.get("skills") if isinstance(parsed.get("skills"), list) else base.skills
        cities = parsed.get("cities") if isinstance(parsed.get("cities"), list) else base.cities
        langs = parsed.get("languages") if isinstance(parsed.get("languages"), list) else base.languages
        seniority = str(parsed.get("seniority_target") or base.seniority_target)
        summary = str(parsed.get("summary") or base.summary)[:300]
        query = " ".join(
            filter(
                None,
                [
                    dream_role,
                    " ".join(str(s) for s in skills),
                    " ".join(str(c) for c in cities),
                    redacted.text[:600],
                ],
            )
        )
        return SeekerProfile(
            skills=[str(s).lower() for s in skills][:20],
            dream_role=dream_role,
            cities=[str(c).lower() for c in cities][:10],
            languages=[str(lang).lower() for lang in langs][:5],
            german_level=str(parsed.get("german_level") or base.german_level),
            visa_status=visa_status,
            seniority_target=seniority,
            summary=summary,
            query_text=query,
            source="llm",
        )
    except Exception:
        return base

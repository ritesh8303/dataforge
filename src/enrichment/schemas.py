"""Canonical enrichment / classification schema — single source of truth.

Filters, prompts, LocalProvider, and OpenAI structured outputs must all use
these enums. A contract test asserts Match filters only check values listed here.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from enrichment.rules_de_en import FIELD_VALUES, SENIORITY_VALUES, VISA_VALUES

FieldLiteral = Literal[
    "ai_ml_data_science",
    "software_engineering",
    "data_analytics",
    "it_support",
    "data_engineering",
    "embedded_systems",
    "cybersecurity",
    "product_management",
    "business_intelligence",
    "qa_testing",
    "cloud_devops",
    "sap_erp",
    "other_tech",
    "non_tech",
]

SeniorityLiteral = Literal[
    "internship",
    "working_student",
    "thesis",
    "trainee_graduate",
    "fresher",
    "junior",
    "mid",
    "senior",
]

VisaLiteral = Literal[
    "sponsorship_offered",
    "relocation_support",
    "existing_permit_required",
    "eu_citizens_only",
    "not_mentioned",
]

WorkModeLiteral = Literal["onsite", "hybrid", "remote"]
LanguageLiteral = Literal["en", "de", "bilingual", "unknown"]

# Visa stances that exclude seekers needing sponsorship / Chancenkarte / student visa.
VISA_BLOCKING_FOR_SEEKER = frozenset({"eu_citizens_only", "existing_permit_required"})

# Profile visa_status values accepted by Match filters.
PROFILE_VISA_STATUS = frozenset(
    {
        "eu_citizen",
        "blue_card",
        "chancenkarte_or_job_seeker",
        "student_visa",
        "needs_visa_from_abroad",
        "",
    }
)

# Aliases the model / older prompts may emit → canonical VISA_VALUES.
_VISA_ALIASES: dict[str, str] = {
    "sponsors": "sponsorship_offered",
    "sponsorship": "sponsorship_offered",
    "sponsorship_available": "sponsorship_offered",
    "relocation": "relocation_support",
    "unclear": "not_mentioned",
    "not_eligible": "eu_citizens_only",
    "eu_only": "eu_citizens_only",
    "permit_required": "existing_permit_required",
}


def normalize_visa_stance(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw or raw in {"nan", "none", "null"}:
        return "not_mentioned"
    mapped = _VISA_ALIASES.get(raw, raw)
    return mapped if mapped in VISA_VALUES else "not_mentioned"


def normalize_field(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in FIELD_VALUES:
        return raw
    return "other_tech" if raw else ""


def normalize_seniority(value: Any) -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "lead": "senior",
        "staff": "senior",
        "principal": "senior",
        "entry": "fresher",
        "entry_level": "fresher",
        "graduate": "trainee_graduate",
        "werkstudent": "working_student",
        "intern": "internship",
        "praktikum": "internship",
    }
    mapped = aliases.get(raw, raw)
    return mapped if mapped in SENIORITY_VALUES else "mid"


class JobEnrichment(BaseModel):
    """Structured classification of one job posting."""

    field: FieldLiteral = "other_tech"
    seniority: SeniorityLiteral = "mid"
    employment_type: str = ""
    skills: list[str] = Field(default_factory=list)
    visa_stance: VisaLiteral = "not_mentioned"
    evidence_visa: str = ""
    languages_required: list[str] = Field(default_factory=list)
    english_ok: bool = False
    entry_level: bool = False
    job_seeker_visa_friendly: bool = False
    is_tech: bool = True
    experience_years_min: int | None = None
    confidence: float = Field(default=0.7, ge=0.0, le=1.0)
    summary: str = ""
    work_mode: WorkModeLiteral = "onsite"
    language: LanguageLiteral = "unknown"
    remote_confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    salary_min: float | None = None
    salary_max: float | None = None
    salary_unit: str | None = None

    @field_validator("visa_stance", mode="before")
    @classmethod
    def _coerce_visa(cls, v: Any) -> str:
        return normalize_visa_stance(v)

    @field_validator("field", mode="before")
    @classmethod
    def _coerce_field(cls, v: Any) -> str:
        return normalize_field(v) or "other_tech"

    @field_validator("seniority", mode="before")
    @classmethod
    def _coerce_seniority(cls, v: Any) -> str:
        return normalize_seniority(v)

    @field_validator("skills", "languages_required", mode="before")
    @classmethod
    def _coerce_list(cls, v: Any) -> list:
        if v is None:
            return []
        if isinstance(v, str):
            try:
                import json

                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return [str(x) for x in parsed]
            except Exception:
                return [v] if v.strip() else []
        if isinstance(v, list):
            return [str(x) for x in v]
        return []

    @field_validator("language", mode="before")
    @classmethod
    def _coerce_language(cls, v: Any) -> str:
        raw = str(v or "unknown").strip().lower()
        aliases = {
            "english": "en",
            "german": "de",
            "deutsch": "de",
            "en": "en",
            "de": "de",
            "bilingual": "bilingual",
            "unknown": "unknown",
        }
        return aliases.get(raw, "unknown")

    @field_validator("work_mode", mode="before")
    @classmethod
    def _coerce_work_mode(cls, v: Any) -> str:
        raw = str(v or "onsite").strip().lower()
        if raw in {"onsite", "hybrid", "remote"}:
            return raw
        if "remote" in raw or "home" in raw:
            return "remote"
        if "hybrid" in raw:
            return "hybrid"
        return "onsite"

    def to_classify_dict(self) -> dict[str, Any]:
        """Shape expected by audience_gate / enricher (pre-ai_ column names)."""
        data = self.model_dump()
        emp = data.get("employment_type") or ""
        if not emp and data["seniority"] in {
            "internship",
            "working_student",
            "thesis",
            "fresher",
        }:
            emp = data["seniority"]
        elif not emp and data["seniority"] in {"trainee_graduate", "junior"}:
            emp = "fresher"
        data["employment_type"] = emp
        data["entry_level"] = data["seniority"] in {
            "internship",
            "working_student",
            "thesis",
            "trainee_graduate",
            "fresher",
            "junior",
        }
        data["field_rule"] = data["field"]
        return data


def openai_enrichment_json_schema() -> dict[str, Any]:
    """JSON Schema for OpenAI response_format=json_schema (strict)."""
    return {
        "name": "job_enrichment",
        "strict": True,
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "field": {"type": "string", "enum": list(FIELD_VALUES)},
                "seniority": {"type": "string", "enum": list(SENIORITY_VALUES)},
                "employment_type": {"type": "string"},
                "skills": {"type": "array", "items": {"type": "string"}},
                "visa_stance": {"type": "string", "enum": list(VISA_VALUES)},
                "evidence_visa": {"type": "string"},
                "languages_required": {"type": "array", "items": {"type": "string"}},
                "english_ok": {"type": "boolean"},
                "entry_level": {"type": "boolean"},
                "job_seeker_visa_friendly": {"type": "boolean"},
                "is_tech": {"type": "boolean"},
                "experience_years_min": {"type": ["integer", "null"]},
                "confidence": {"type": "number"},
                "summary": {"type": "string"},
                "work_mode": {"type": "string", "enum": ["onsite", "hybrid", "remote"]},
                "language": {"type": "string", "enum": ["en", "de", "bilingual", "unknown"]},
                "remote_confidence": {"type": "number"},
                "salary_min": {"type": ["number", "null"]},
                "salary_max": {"type": ["number", "null"]},
                "salary_unit": {"type": ["string", "null"]},
            },
            "required": [
                "field",
                "seniority",
                "employment_type",
                "skills",
                "visa_stance",
                "evidence_visa",
                "languages_required",
                "english_ok",
                "entry_level",
                "job_seeker_visa_friendly",
                "is_tech",
                "experience_years_min",
                "confidence",
                "summary",
                "work_mode",
                "language",
                "remote_confidence",
                "salary_min",
                "salary_max",
                "salary_unit",
            ],
        },
    }


def parse_enrichment_payload(raw: dict[str, Any] | None) -> JobEnrichment | None:
    if not isinstance(raw, dict):
        return None
    try:
        return JobEnrichment.model_validate(raw)
    except Exception:
        return None

"""Rule-based DE/EN classifiers for tech field, seniority, visa, language.

Used as a free pre-filter before LLM enrichment and as a measurable baseline
for thesis RQ2 (rule vs LLM extraction F1).
"""

from __future__ import annotations

import re
from typing import Any

FIELD_VALUES = (
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
)

SENIORITY_VALUES = (
    "internship",
    "working_student",
    "thesis",
    "trainee_graduate",
    "junior",
    "mid",
    "senior",
)

# Product surface: early-career seeker roles only (thesis / DataForge Jobs+Match).
PRODUCT_SENIORITY = frozenset(
    {
        "internship",
        "working_student",
        "thesis",
        "trainee_graduate",
        "junior",
    }
)

# Data / AI / closely related evolving fields for the seeker product.
PRODUCT_DATA_AI_FIELDS = frozenset(
    {
        "ai_ml_data_science",
        "data_engineering",
        "data_analytics",
        "business_intelligence",
        "cloud_devops",  # data platform / MLOps-adjacent when classified here
    }
)

EMPLOYMENT_TYPE_BY_SENIORITY = {
    "working_student": "working_student",
    "internship": "internship",
    "thesis": "thesis",
    "trainee_graduate": "fresher",
    "junior": "fresher",
}

VISA_VALUES = (
    "sponsorship_offered",
    "relocation_support",
    "existing_permit_required",
    "eu_citizens_only",
    "not_mentioned",
)

# Ordered: first match wins for field classification.
_FIELD_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (
        "ai_ml_data_science",
        re.compile(
            r"\b("
            r"machine\s*learning|deep\s*learning|data\s*scientist|ml\s*engineer|"
            r"nlp|computer\s*vision|llm|gen(?:erative)?\s*ai|ki\s*ingenieur|"
            r"artificial\s*intelligence|forschung.*ki|ai\s*/\s*ml|mlops|"
            r"data\s*&\s*ai|ai\s*&\s*data|data\s*and\s*ai|ai\s*solutions|"
            r"scientific\s*computing|physical\s*ai|ki\s*in\s+der|"
            r"datenanalyse\s*mit\s*ki"
            r")\b",
            re.I,
        ),
    ),
    (
        "data_engineering",
        re.compile(
            r"\b("
            r"data\s*engineer|dateningenieur|etl|elt|spark|airflow|dbt|"
            r"data\s*platform|lakehouse|kafka|streaming\s*data|"
            r"pipeline\s*engineer|datenplattform|"
            r"analytics\s*&\s*data\s*engineering|data\s*engineering"
            r")\b",
            re.I,
        ),
    ),
    (
        "data_analytics",
        re.compile(
            r"\b("
            r"data\s*analyst|datenanalyst|analytics\s*engineer|"
            r"business\s*analyst.*data|sql\s*analyst|reporting\s*analyst|"
            r"data\s*analytics|datenanalyse|daten[\s-]*und[\s-]*prozessanalyse|"
            r"fachinformatiker.*daten|data\s*analysis"
            r")\b",
            re.I,
        ),
    ),
    (
        "business_intelligence",
        re.compile(
            r"\b("
            r"business\s*intelligence|\bbi\b|power\s*bi|tableau|"
            r"looker|qlik|bi\s*developer|bi\s*analyst|bi\s*engineer"
            r")\b",
            re.I,
        ),
    ),
    (
        "cloud_devops",
        re.compile(
            r"\b("
            r"devops|sre|site\s*reliability|platform\s*engineer|"
            r"cloud\s*engineer|kubernetes|k8s|terraform|ansible|"
            r"aws\s*engineer|azure\s*engineer|gcp\s*engineer|ci/?cd"
            r")\b",
            re.I,
        ),
    ),
    (
        "cybersecurity",
        re.compile(
            r"\b("
            r"security\s*engineer|cyber\s*security|cybersecurity|infosec|"
            r"pentester|penetration\s*test|appsec|soc\s*analyst|"
            r"it[\s-]*sicherheit|informationssicherheit"
            r")\b",
            re.I,
        ),
    ),
    (
        "embedded_systems",
        re.compile(
            r"\b("
            r"embedded|firmware|fpga|rtos|mikrocontroller|microcontroller|"
            r"embedded\s*c|automotive\s*software|ecu\b"
            r")\b",
            re.I,
        ),
    ),
    (
        "qa_testing",
        re.compile(
            r"\b("
            r"qa\s*engineer|quality\s*assurance|test\s*engineer|"
            r"software\s*tester|sdet|testautomatisierung|automation\s*tester"
            r")\b",
            re.I,
        ),
    ),
    (
        "sap_erp",
        re.compile(
            r"\b("
            r"\bsap\b|abap|s/?4\s*hana|erp\s*consultant|sap\s*basis|"
            r"sap\s*consultant|sap\s*entwickler"
            r")\b",
            re.I,
        ),
    ),
    (
        "product_management",
        re.compile(
            r"\b("
            r"product\s*manager|produktmanager|product\s*owner|"
            r"technical\s*product|tpm\b|produktmanagement"
            r")\b",
            re.I,
        ),
    ),
    (
        "it_support",
        re.compile(
            r"\b("
            r"it\s*support|helpdesk|help\s*desk|desktop\s*support|"
            r"systemadministrator|sysadmin|it\s*administrator|"
            r"servicedesk|service\s*desk|it[\s-]*betreuung"
            r")\b",
            re.I,
        ),
    ),
    (
        "software_engineering",
        re.compile(
            r"\b("
            r"software\s*engineer|software\s*entwickler|backend|frontend|"
            r"full[\s-]?stack|developer|entwickler|programmer|"
            r"java\s*engineer|python\s*developer|golang|typescript|"
            r"react|node\.?js|microservice"
            r")\b",
            re.I,
        ),
    ),
]

_OTHER_TECH_RE = re.compile(
    r"\b("
    r"engineer|developer|entwickler|programmer|analyst|scientist|"
    r"architect|devops|sre|data|software|cloud|tech|it\b|digital|"
    r"informatik|computer\s*science|agile|scrum|api|saas"
    r")\b",
    re.I,
)

_SENIORITY_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (
        "working_student",
        re.compile(
            r"\b("
            r"werkstudent|working\s*student|studentische\s*hilfskraft|"
            r"student\s*assistant|werkstudent:in|working\s*student:in"
            r")\b",
            re.I,
        ),
    ),
    (
        "thesis",
        re.compile(
            r"\b("
            r"masterarbeit|bachelorarbeit|abschlussarbeit|"
            r"master\s*thesis|bachelor\s*thesis|thesis\s*(?:student|role|position)|"
            r"diplomarbeit|abschluss\s*arbeit"
            r")\b",
            re.I,
        ),
    ),
    (
        "internship",
        re.compile(
            r"\b("
            r"praktikant|praktikum|internship|intern\b|pflichtpraktikum|"
            r"working\s*intern"
            r")\b",
            re.I,
        ),
    ),
    (
        "trainee_graduate",
        re.compile(
            r"\b("
            r"trainee|graduate\s*program|absolvent|berufseinsteiger|"
            r"entry[\s-]?level|new\s*grad|graduate\s*scheme|"
            r"einstiegsposition|nachwuchskraft|fresher"
            r")\b",
            re.I,
        ),
    ),
    (
        "junior",
        re.compile(r"\b(junior|jr\.?|einsteiger)\b", re.I),
    ),
    (
        "senior",
        re.compile(r"\b(senior|sr\.?|staff|principal|lead|leiter|head\s+of)\b", re.I),
    ),
]

_EXP_YEARS_RE = re.compile(
    r"(?:(?:mindestens|min\.?|at\s+least|über|more\s+than)\s*)?"
    r"(\d+)\s*(?:\+|plus)?\s*(?:Jahre|years?|yrs?)"
    r"(?:\s+(?:Berufserfahrung|experience|of\s+experience))?",
    re.I,
)

_VISA_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (
        "eu_citizens_only",
        re.compile(
            r"("
            r"eu[\s/-]*b[uü]rger|eu\s*citizens?\s+only|"
            r"only\s+(?:for\s+)?(?:eu|eea)\s+citizens|"
            r"nur\s+(?:f[uü]r\s+)?eu[\s/-]*angeh[oö]rige|"
            r"work\s+permit\s+(?:already\s+)?required|"
            r"g[uü]ltige\s+arbeitserlaubnis\s+erforderlich|"
            r"must\s+(?:already\s+)?have\s+(?:a\s+)?(?:valid\s+)?(?:work|residence)\s+permit|"
            r"no\s+visa\s+sponsorship"
            r")",
            re.I,
        ),
    ),
    (
        "existing_permit_required",
        re.compile(
            r"("
            r"existing\s+(?:work\s+)?permit|"
            r"bereits\s+(?:eine\s+)?(?:g[uü]ltige\s+)?arbeitserlaubnis|"
            r"valid\s+(?:german\s+)?work\s+authorization\s+required|"
            r"must\s+be\s+eligible\s+to\s+work"
            r")",
            re.I,
        ),
    ),
    (
        "sponsorship_offered",
        re.compile(
            r"("
            r"visa\s+sponsorship|sponsor(?:s|ship)?\s+(?:visa|work\s+permit)|"
            r"blue\s+card|eu\s+blue\s+card|blauen?\s+karte|"
            r"visumunterst[uü]tzung|arbeitsvisum\s+m[oö]glich|"
            r"we\s+(?:can|will)\s+sponsor"
            r")",
            re.I,
        ),
    ),
    (
        "relocation_support",
        re.compile(
            r"("
            r"relocation\s+(?:support|package|assistance)|"
            r"umzugshilfe|umzugsunterst[uü]tzung|relocation\s+bonus"
            r")",
            re.I,
        ),
    ),
]

_GERMAN_REQ_RE = re.compile(
    r"("
    r"(?:fluen(?:t|cy)|native|verhandlungssicher|flie[sß]end|"
    r"sehr\s+gute|gute|solid)\s+(?:in\s+)?(?:deutsch|german)"
    r"|german\s+(?:at\s+least\s+)?(?:[abc][12]|c1|c2|b2|b1)"
    r"|deutsch\s*(?:kenntnisse)?\s*(?:auf\s+)?(?:niveau\s+)?(?:[abc][12]|c1|c2|b2)"
    r"|deutsch\s+als\s+muttersprache"
    r")",
    re.I,
)

_ENGLISH_OK_RE = re.compile(
    r"("
    r"english\s+(?:is\s+)?(?:enough|sufficient|fine|ok)|"
    r"english[\s-]*speaking|working\s+language\s*(?:is\s+)?english|"
    r"english\s+(?:required|fluent)|company\s+language\s*:?\s*english|"
    r"deutsch\s+(?:nicht|no)\s+(?:erforderlich|required)|"
    r"no\s+german\s+required|german\s+(?:not\s+required|optional)"
    r")",
    re.I,
)

_CEFR_RE = re.compile(
    r"(?:deutsch|german|englisch|english)\s*(?:kenntnisse)?\s*"
    r"(?:auf\s+)?(?:niveau\s+)?([ABC][12])",
    re.I,
)


def _blob(title: str = "", description: str = "", tags: str = "") -> str:
    return f"{title or ''}\n{tags or ''}\n{description or ''}"


def classify_field(title: str = "", description: str = "", tags: str = "") -> dict[str, Any]:
    """Return field enum + confidence + rule/ambiguous flag."""
    text = _blob(title, description, tags)
    title_tags = f"{title or ''}\n{tags or ''}"

    for field, pattern in _FIELD_PATTERNS:
        if pattern.search(title_tags) or pattern.search(text):
            return {
                "field": field,
                "field_rule": field,
                "is_tech": True,
                "confidence": 0.9 if pattern.search(title_tags) else 0.75,
            }

    if _OTHER_TECH_RE.search(title_tags):
        return {
            "field": "other_tech",
            "field_rule": "other_tech",
            "is_tech": True,
            "confidence": 0.55,
        }

    if _OTHER_TECH_RE.search(text) and len(text) > 80:
        return {
            "field": "other_tech",
            "field_rule": "ambiguous",
            "is_tech": True,
            "confidence": 0.4,
        }

    return {
        "field": "non_tech",
        "field_rule": "non_tech",
        "is_tech": False,
        "confidence": 0.85,
    }


def classify_seniority(title: str = "", description: str = "") -> dict[str, Any]:
    text = _blob(title, description)
    for seniority, pattern in _SENIORITY_PATTERNS:
        if pattern.search(title or "") or pattern.search(text):
            return {"seniority": seniority, "confidence": 0.9 if pattern.search(title or "") else 0.7}
    return {"seniority": "mid", "confidence": 0.35, "field_rule_note": "default_mid"}


def classify_experience_years(title: str = "", description: str = "") -> int | None:
    text = _blob(title, description)
    matches = [int(m.group(1)) for m in _EXP_YEARS_RE.finditer(text)]
    if not matches:
        return None
    return min(matches)


def classify_visa_stance(title: str = "", description: str = "") -> dict[str, Any]:
    text = _blob(title, description)
    for stance, pattern in _VISA_PATTERNS:
        m = pattern.search(text)
        if m:
            return {
                "visa_stance": stance,
                "evidence_visa": m.group(0)[:240],
                "confidence": 0.85,
            }
    return {"visa_stance": "not_mentioned", "evidence_visa": "", "confidence": 0.6}


def classify_languages(title: str = "", description: str = "") -> dict[str, Any]:
    text = _blob(title, description)
    languages: list[dict[str, str]] = []
    english_ok = bool(_ENGLISH_OK_RE.search(text))

    for m in _CEFR_RE.finditer(text):
        span = text[max(0, m.start() - 20) : m.end()].lower()
        lang = "de" if ("deutsch" in span or "german" in span) else "en"
        languages.append({"lang": lang, "cefr": m.group(1).upper()})

    german_required = bool(_GERMAN_REQ_RE.search(text))
    if german_required and not any(x["lang"] == "de" for x in languages):
        languages.append({"lang": "de", "cefr": "B2"})

    if not english_ok and not german_required:
        # Default international tech boards often work in English when unspecified.
        english_ok = bool(re.search(r"\benglish\b", text, re.I)) and not german_required

    return {
        "languages_required": languages,
        "english_ok": english_ok or (not german_required and bool(re.search(r"\b(remote|english)\b", text, re.I))),
        "german_required": german_required,
    }


def derive_flags(
    seniority: str,
    experience_years_min: int | None,
    visa_stance: str,
    english_ok: bool,
    german_required: bool,
    salary_annual: float | None = None,
) -> dict[str, Any]:
    """Derived portfolio/thesis flags (advisory, not legal advice)."""
    entry_level = seniority in PRODUCT_SENIORITY or (
        experience_years_min is not None and experience_years_min <= 1
    )
    if experience_years_min is not None and experience_years_min > 2:
        entry_level = seniority in {"internship", "working_student", "thesis", "trainee_graduate"}

    blue_card_new_grad_eligible = None
    if salary_annual is not None:
        blue_card_new_grad_eligible = salary_annual >= 45934.20

    job_seeker_visa_friendly = bool(
        entry_level
        and visa_stance not in {"eu_citizens_only", "existing_permit_required"}
        and (english_ok or not german_required)
    )

    return {
        "entry_level": entry_level,
        "employment_type": EMPLOYMENT_TYPE_BY_SENIORITY.get(seniority, ""),
        "blue_card_new_grad_eligible": blue_card_new_grad_eligible,
        "job_seeker_visa_friendly": job_seeker_visa_friendly,
    }


def is_product_data_ai_field(field: str) -> bool:
    return str(field or "").strip().lower() in PRODUCT_DATA_AI_FIELDS


def is_product_seniority(seniority: str) -> bool:
    return str(seniority or "").strip().lower() in PRODUCT_SENIORITY


def classify_job(title: str = "", description: str = "", tags: str = "") -> dict[str, Any]:
    """Full rule-based classification for one job posting."""
    field = classify_field(title, description, tags)
    seniority = classify_seniority(title, description)
    years = classify_experience_years(title, description)
    visa = classify_visa_stance(title, description)
    langs = classify_languages(title, description)
    flags = derive_flags(
        seniority=seniority["seniority"],
        experience_years_min=years,
        visa_stance=visa["visa_stance"],
        english_ok=langs["english_ok"],
        german_required=langs["german_required"],
    )
    return {
        **field,
        "seniority": seniority["seniority"],
        "experience_years_min": years,
        "visa_stance": visa["visa_stance"],
        "evidence_visa": visa.get("evidence_visa", ""),
        "languages_required": langs["languages_required"],
        "english_ok": langs["english_ok"],
        "german_required": langs["german_required"],
        **flags,
    }

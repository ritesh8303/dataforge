"""Product audience gate: EU + data/AI field + fresher/WS/thesis only.

DataForge's public Gold / Jobs / Match surface is for early-career seekers in
data and AI-related roles across the European Union — not a general tech board.
"""

from __future__ import annotations

from typing import Any

from enrichment.rules_de_en import (
    PRODUCT_DATA_AI_FIELDS,
    PRODUCT_SENIORITY,
    classify_job,
    is_product_data_ai_field,
    is_product_seniority,
)

# EU member states (ISO-2 + common English/German names). UK/CH/NO are out of product scope.
EU_COUNTRY_TOKENS = frozenset(
    {
        "at",
        "be",
        "bg",
        "cy",
        "cz",
        "de",
        "dk",
        "ee",
        "es",
        "fi",
        "fr",
        "gr",
        "el",
        "hr",
        "hu",
        "ie",
        "it",
        "lt",
        "lu",
        "lv",
        "mt",
        "nl",
        "pl",
        "pt",
        "ro",
        "se",
        "si",
        "sk",
        "austria",
        "belgium",
        "bulgaria",
        "croatia",
        "cyprus",
        "czechia",
        "czech republic",
        "denmark",
        "estonia",
        "finland",
        "france",
        "germany",
        "greece",
        "hungary",
        "ireland",
        "italy",
        "latvia",
        "lithuania",
        "luxembourg",
        "malta",
        "netherlands",
        "poland",
        "portugal",
        "romania",
        "slovakia",
        "slovenia",
        "spain",
        "sweden",
        "österreich",
        "belgien",
        "bulgarien",
        "kroatien",
        "zypern",
        "tschechien",
        "dänemark",
        "estland",
        "finnland",
        "frankreich",
        "deutschland",
        "griechenland",
        "ungarn",
        "irland",
        "italien",
        "lettland",
        "litauen",
        "luxemburg",
        "niederlande",
        "polen",
        "rumänien",
        "slowakei",
        "slowenien",
        "spanien",
        "schweden",
    }
)

# Non-EU European tokens — still Europe in lakehouse, but out of seeker product.
NON_EU_EUROPE_BLOCK = frozenset(
    {
        "uk",
        "gb",
        "united kingdom",
        "great britain",
        "england",
        "scotland",
        "wales",
        "london",
        "ch",
        "switzerland",
        "schweiz",
        "zurich",
        "zürich",
        "geneva",
        "no",
        "norway",
        "norwegen",
        "oslo",
        "is",
        "iceland",
        "li",
        "liechtenstein",
    }
)


def _norm(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip().lower()
    if s in {"nan", "<na>", "none", "null"}:
        return ""
    return s


def is_eu_location(
    location: str = "",
    region: str = "",
    title: str = "",
    *,
    allow_remote_eu: bool = True,
) -> bool:
    """True when the posting is in an EU member state (or EU-remote)."""
    blob = " ".join(filter(None, [_norm(location), _norm(region), _norm(title)]))
    if not blob:
        return False
    if any(tok in blob for tok in NON_EU_EUROPE_BLOCK):
        # Explicit non-EU European → out (product is EU-only).
        if not any(tok in blob for tok in EU_COUNTRY_TOKENS):
            return False
    for tok in EU_COUNTRY_TOKENS:
        if len(tok) <= 2:
            # ISO codes: word boundary-ish
            if f" {tok} " in f" {blob} " or blob.endswith(f" {tok}") or blob.startswith(f"{tok} "):
                return True
            if blob == tok:
                return True
        elif tok in blob:
            return True
    if allow_remote_eu and ("remote" in blob or "home office" in blob or "homeoffice" in blob):
        # Remote without country: keep if region already EU or location empty-ish EU remote boards.
        reg = _norm(region)
        if reg in EU_COUNTRY_TOKENS or reg in {"eu", "europe", "european union"}:
            return True
    return False


def classify_for_audience(job: dict[str, Any]) -> dict[str, Any]:
    """Run rules classify and attach product decision fields."""
    title = str(job.get("title") or "")
    description = str(job.get("description") or "")
    tags = str(job.get("tags") or "")
    rules = classify_job(title, description, tags)

    field = _norm(job.get("ai_field") or job.get("ai_field_rule") or job.get("field_rule") or rules.get("field"))
    seniority = _norm(job.get("ai_seniority") or job.get("seniority_rule") or rules.get("seniority"))
    if field in {"", "nan"}:
        field = _norm(rules.get("field"))
    if seniority in {"", "nan"}:
        seniority = _norm(rules.get("seniority"))

    eu_ok = is_eu_location(
        location=str(job.get("location") or ""),
        region=str(job.get("region") or ""),
        title=title,
    )
    field_ok = is_product_data_ai_field(field)
    seniority_ok = is_product_seniority(seniority)

    # High-experience mid/senior defaults fail seniority_ok; also reject explicit senior + years>2
    years = rules.get("experience_years_min")
    if years is not None and int(years) > 2 and seniority not in {"internship", "working_student", "thesis"}:
        seniority_ok = False

    conf_field = 0.7
    if isinstance(rules.get("confidence"), (int, float)):
        conf_field = float(rules["confidence"])
    field_rule = str(rules.get("field_rule") or "")
    uncertain = field_rule == "ambiguous" or (
        seniority == "mid" and conf_field < 0.5 and not seniority_ok
    )

    accept = eu_ok and field_ok and seniority_ok and not uncertain
    reasons: list[str] = []
    if not eu_ok:
        reasons.append("not_eu")
    if not field_ok:
        reasons.append("not_data_ai_field")
    if not seniority_ok:
        reasons.append("not_fresher_ws_thesis")
    if uncertain:
        reasons.append("ambiguous_needs_review")

    return {
        **rules,
        "field": field or rules.get("field"),
        "seniority": seniority or rules.get("seniority"),
        "employment_type": rules.get("employment_type")
        or ({"working_student": "working_student", "internship": "internship", "thesis": "thesis"}.get(seniority, "fresher") if seniority_ok else ""),
        "audience_eu": eu_ok,
        "audience_data_ai": field_ok,
        "audience_seniority": seniority_ok,
        "audience_uncertain": uncertain,
        "audience_accept": accept,
        "audience_reject_reasons": reasons,
        "product_fields": sorted(PRODUCT_DATA_AI_FIELDS),
        "product_seniorities": sorted(PRODUCT_SENIORITY),
    }


def passes_audience(job: dict[str, Any], *, classify_if_needed: bool = True) -> bool:
    if job.get("audience_accept") is not None and not classify_if_needed:
        return bool(job.get("audience_accept"))
    return bool(classify_for_audience(job).get("audience_accept"))


def filter_jobs_for_product(jobs: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split into (accepted_for_product, review_or_rejected)."""
    accepted: list[dict[str, Any]] = []
    other: list[dict[str, Any]] = []
    for job in jobs:
        decision = classify_for_audience(job)
        enriched = {**job, **{k: decision[k] for k in decision if k.startswith("audience_") or k in {"employment_type", "seniority", "field", "entry_level", "english_ok"}}}
        enriched["ai_seniority"] = decision.get("seniority")
        enriched["ai_field"] = decision.get("field")
        enriched["ai_entry_level"] = decision.get("entry_level")
        enriched["ai_english_ok"] = decision.get("english_ok")
        if decision.get("audience_accept"):
            accepted.append(enriched)
        else:
            other.append(enriched)
    return accepted, other

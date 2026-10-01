"""Lightweight ESCO-inspired skill / occupation normalizer (offline subset).

Full ESCO download is optional; this ships a curated DE/EN mapping aligned with
early-career data & AI roles so Match / enrichment stay dependency-light.
"""

from __future__ import annotations

import re
from typing import Any

# Canonical skill id → display labels (en, de) + aliases
ESCO_SKILL_MAP: dict[str, dict[str, Any]] = {
    "python": {"en": "Python", "de": "Python", "aliases": ["python3", "py"]},
    "sql": {"en": "SQL", "de": "SQL", "aliases": ["t-sql", "pl/sql", "postgresql", "postgres", "mysql"]},
    "spark": {"en": "Apache Spark", "de": "Apache Spark", "aliases": ["pyspark", "spark sql"]},
    "airflow": {"en": "Apache Airflow", "de": "Apache Airflow", "aliases": ["airflow dag"]},
    "dbt": {"en": "dbt", "de": "dbt", "aliases": ["data build tool"]},
    "aws": {"en": "Amazon Web Services", "de": "Amazon Web Services", "aliases": ["amazon aws", "s3", "redshift"]},
    "azure": {"en": "Microsoft Azure", "de": "Microsoft Azure", "aliases": ["azure data factory", "adf"]},
    "gcp": {"en": "Google Cloud", "de": "Google Cloud", "aliases": ["google cloud platform", "bigquery"]},
    "kafka": {"en": "Apache Kafka", "de": "Apache Kafka", "aliases": ["kafka streams"]},
    "pandas": {"en": "pandas", "de": "pandas", "aliases": []},
    "pytorch": {"en": "PyTorch", "de": "PyTorch", "aliases": ["torch"]},
    "tensorflow": {"en": "TensorFlow", "de": "TensorFlow", "aliases": ["keras"]},
    "docker": {"en": "Docker", "de": "Docker", "aliases": ["containerization"]},
    "kubernetes": {"en": "Kubernetes", "de": "Kubernetes", "aliases": ["k8s"]},
    "terraform": {"en": "Terraform", "de": "Terraform", "aliases": ["iac"]},
    "snowflake": {"en": "Snowflake", "de": "Snowflake", "aliases": []},
    "databricks": {"en": "Databricks", "de": "Databricks", "aliases": []},
    "mlflow": {"en": "MLflow", "de": "MLflow", "aliases": []},
    "power_bi": {"en": "Power BI", "de": "Power BI", "aliases": ["powerbi", "power-bi"]},
    "tableau": {"en": "Tableau", "de": "Tableau", "aliases": []},
    "java": {"en": "Java", "de": "Java", "aliases": []},
    "scala": {"en": "Scala", "de": "Scala", "aliases": []},
    "r": {"en": "R", "de": "R", "aliases": ["rstudio"]},
    "nlp": {"en": "Natural language processing", "de": "Verarbeitung natürlicher Sprache", "aliases": ["natural language processing"]},
    "llm": {"en": "Large language models", "de": "Große Sprachmodelle", "aliases": ["large language model", "genai", "generative ai"]},
    "machine_learning": {
        "en": "Machine learning",
        "de": "Maschinelles Lernen",
        "aliases": ["ml", "maschinelles lernen"],
    },
    "deep_learning": {"en": "Deep learning", "de": "Deep Learning", "aliases": ["neural networks"]},
    "etl": {"en": "ETL", "de": "ETL", "aliases": ["elt", "data pipeline"]},
    "data_engineering": {
        "en": "Data engineering",
        "de": "Data Engineering",
        "aliases": ["dateningenieur", "datenplattform"],
    },
    "data_analysis": {
        "en": "Data analysis",
        "de": "Datenanalyse",
        "aliases": ["datenanalyse", "data analytics"],
    },
}

# Occupation → preferred skill ids (thin ESCO occupation proxy)
ESCO_OCCUPATION_SKILLS: dict[str, list[str]] = {
    "data_engineering": ["python", "sql", "spark", "airflow", "dbt", "aws", "kafka", "etl"],
    "ai_ml_data_science": ["python", "pytorch", "tensorflow", "machine_learning", "nlp", "llm", "mlflow"],
    "data_analytics": ["sql", "python", "pandas", "tableau", "power_bi", "data_analysis"],
    "business_intelligence": ["sql", "power_bi", "tableau", "data_analysis"],
    "cloud_devops": ["aws", "azure", "gcp", "docker", "kubernetes", "terraform"],
}


def _alias_index() -> dict[str, str]:
    idx: dict[str, str] = {}
    for sid, meta in ESCO_SKILL_MAP.items():
        idx[sid.replace("_", " ")] = sid
        idx[sid] = sid
        idx[str(meta["en"]).lower()] = sid
        idx[str(meta["de"]).lower()] = sid
        for a in meta.get("aliases") or []:
            idx[str(a).lower()] = sid
    return idx


_ALIAS_INDEX = _alias_index()


def normalize_skill_token(token: str) -> str | None:
    t = re.sub(r"\s+", " ", (token or "").strip().lower())
    if not t:
        return None
    if t in _ALIAS_INDEX:
        return _ALIAS_INDEX[t]
    # soft contains for multi-word
    for alias, sid in _ALIAS_INDEX.items():
        if len(alias) >= 3 and alias in t:
            return sid
    return None


def extract_esco_skills(text: str, *, limit: int = 15) -> list[dict[str, str]]:
    """Return [{id, en, de}] for skills mentioned in text."""
    blob = f" {(text or '').lower()} "
    found: list[str] = []
    for sid, meta in ESCO_SKILL_MAP.items():
        needles = [sid.replace("_", " "), str(meta["en"]).lower(), str(meta["de"]).lower()]
        needles.extend(str(a).lower() for a in (meta.get("aliases") or []))
        if any(n and n in blob for n in needles):
            found.append(sid)
    out = []
    for sid in found[:limit]:
        meta = ESCO_SKILL_MAP[sid]
        out.append({"id": sid, "en": meta["en"], "de": meta["de"]})
    return out


def normalize_skill_list(skills: list[Any]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in skills or []:
        sid = normalize_skill_token(str(raw))
        if not sid or sid in seen:
            continue
        seen.add(sid)
        meta = ESCO_SKILL_MAP[sid]
        out.append({"id": sid, "en": meta["en"], "de": meta["de"]})
    return out


def occupation_skill_gap(field: str, resume_skills: list[str]) -> dict[str, Any]:
    wanted = ESCO_OCCUPATION_SKILLS.get(str(field or "").lower(), [])
    have = {normalize_skill_token(s) for s in resume_skills}
    have.discard(None)
    missing = [s for s in wanted if s not in have]
    matched = [s for s in wanted if s in have]
    return {
        "occupation_field": field,
        "matched": matched,
        "missing": missing,
        "labels": {
            "matched": [ESCO_SKILL_MAP[s]["en"] for s in matched if s in ESCO_SKILL_MAP],
            "missing": [ESCO_SKILL_MAP[s]["en"] for s in missing if s in ESCO_SKILL_MAP],
        },
    }

"""Normalize and validate company names across the pipeline."""
import re
from typing import Optional

_INVALID_COMPANY_RE = re.compile(
    r"^(non\s+renseigné|unknown(\s+employer)?|not\s+specified|n/?a|none|confidential|"
    r"startup|unbekannt|nicht\s+angegeben|anonym|tbd|various|diverse|keine\s+angabe|"
    r"not\s+provided|unspecified|company|employer)$",
    re.IGNORECASE,
)


def normalize_company(name: Optional[str]) -> Optional[str]:
    """
    Return a cleaned company name, or None if the value is a placeholder/invalid.
    """
    if name is None:
        return None
    cleaned = str(name).strip()
    if not cleaned or cleaned.lower() in {"nan", "<na>", "none", "null"}:
        return None
    if _INVALID_COMPANY_RE.match(cleaned):
        return None
    return cleaned

"""PII redaction before resume/text is sent to an LLM."""

from __future__ import annotations

import re
from dataclasses import dataclass


EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
PHONE_RE = re.compile(
    r"(?:\+|00)?\d{1,3}[\s./-]?(?:\(?\d{2,4}\)?[\s./-]?)?\d{3,4}[\s./-]?\d{3,4}\b"
)
# Common "Name: Alice Example" / LinkedIn profile URL patterns
NAME_LINE_RE = re.compile(
    r"(?im)^(?:name|full\s*name|candidate)\s*[:\-]\s*.+$"
)
LINKEDIN_RE = re.compile(r"https?://(?:www\.)?linkedin\.com/in/[A-Za-z0-9_-]+/?", re.I)
IBAN_RE = re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b")


@dataclass
class RedactionResult:
    text: str
    redacted_counts: dict[str, int]

    @property
    def total(self) -> int:
        return sum(self.redacted_counts.values())


def redact_pii(text: str) -> RedactionResult:
    """Replace emails, phones, LinkedIn URLs, name lines, IBANs with placeholders."""
    if not text:
        return RedactionResult(text="", redacted_counts={})

    counts = {"email": 0, "phone": 0, "linkedin": 0, "name_line": 0, "iban": 0}
    out = text

    def _sub(pattern: re.Pattern[str], key: str, repl: str, s: str) -> str:
        def replacer(m: re.Match[str]) -> str:
            counts[key] += 1
            return repl

        return pattern.sub(replacer, s)

    out = _sub(EMAIL_RE, "email", "[EMAIL]", out)
    out = _sub(LINKEDIN_RE, "linkedin", "[LINKEDIN]", out)
    out = _sub(IBAN_RE, "iban", "[IBAN]", out)
    out = _sub(NAME_LINE_RE, "name_line", "Name: [REDACTED]", out)
    # Phones last — avoid mangling numbers inside already-redacted tokens
    out = _sub(PHONE_RE, "phone", "[PHONE]", out)
    return RedactionResult(text=out, redacted_counts=counts)

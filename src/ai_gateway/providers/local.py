"""Local fallback provider — no API keys, deterministic, for tests and offline mode."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter

from ai_gateway.providers.base import BaseProvider, timed_call
from ai_gateway.types import EmbeddingResponse, ProviderResponse
from enrichment.rules_de_en import FIELD_VALUES, SENIORITY_VALUES, VISA_VALUES

_TOKEN_RE = re.compile(r"[a-zA-Z0-9+#./]+")
_EMBED_DIM = 128


def _tokenize(text: str) -> list[str]:
    return [t.lower() for t in _TOKEN_RE.findall(text or "")]


def _hash_embed(text: str, dim: int = _EMBED_DIM) -> list[float]:
    """Deterministic sparse hash embedding — thesis local baseline without API deps."""
    tokens = _tokenize(text)
    if not tokens:
        return [0.0] * dim
    vec = [0.0] * dim
    for tok in tokens:
        h = int(hashlib.md5(tok.encode()).hexdigest(), 16)
        idx = h % dim
        sign = 1.0 if (h >> 1) % 2 == 0 else -1.0
        vec[idx] += sign
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


class LocalProvider(BaseProvider):
    name = "local"

    def complete(self, prompt: str, system: str = "", model: str | None = None, **kwargs) -> ProviderResponse:
        def _run():
            task = kwargs.get("task", "summarize")
            if task == "explain":
                return self._explain_json(prompt)
            if task == "rerank":
                return self._rerank_json(prompt)
            if task == "enrich" or kwargs.get("json_schema"):
                return self._enrich_json(prompt)
            if kwargs.get("json_mode"):
                blob = f"{prompt}\n{system}".lower()
                if any(k in blob for k in ("visa_stance", "job posting", "seniority", "classify")):
                    return self._enrich_json(prompt)
                return self._enrich_json(prompt)
            return self._summarize(prompt)

        text, latency_ms = timed_call(_run)
        return ProviderResponse(
            text=text,
            model_id=model or "local-heuristic",
            provider=self.name,
            input_tokens=len(_tokenize(prompt + system)),
            output_tokens=len(_tokenize(text)),
            latency_ms=latency_ms,
        )

    def embed(self, text: str, model: str | None = None, **kwargs) -> EmbeddingResponse:
        vec, latency_ms = timed_call(_hash_embed, text)
        return EmbeddingResponse(
            vector=vec,
            model_id=model or "local-tfidf",
            provider=self.name,
            input_tokens=len(_tokenize(text)),
            latency_ms=latency_ms,
        )

    def _summarize(self, prompt: str) -> str:
        tokens = _tokenize(prompt)
        if not tokens:
            return "No content to summarize."
        top = Counter(tokens).most_common(8)
        keywords = ", ".join(w for w, _ in top)
        return f"Role summary (local): focuses on {keywords}."

    def _explain_json(self, prompt: str) -> str:
        try:
            data = json.loads(prompt)
            jid = str(data.get("job_id") or "")
            title = str(data.get("title") or "")
            desc = str(data.get("description") or "")
            evidence = (title or desc)[:120]
            return json.dumps(
                {
                    "job_id": jid,
                    "reason": f"Title/description aligns with target role ({title[:80]}).",
                    "evidence": evidence,
                }
            )
        except Exception:
            return json.dumps({"job_id": "", "reason": "Retrieved by local heuristic.", "evidence": ""})

    def _rerank_json(self, prompt: str) -> str:
        try:
            data = json.loads(prompt)
            jobs = data.get("jobs") or []
            scores = []
            for j in jobs:
                scores.append(
                    {
                        "job_id": j.get("job_id"),
                        "score": 0.7,
                        "note": "local heuristic",
                    }
                )
            return json.dumps({"scores": scores})
        except Exception:
            return json.dumps({"scores": []})

    def _enrich_json(self, prompt: str) -> str:
        text = prompt.lower()
        skills = []
        for kw in [
            "python", "sql", "aws", "spark", "kafka", "docker", "kubernetes",
            "machine learning", "llm", "pytorch", "terraform", "java", "scala",
            "dbt", "airflow", "pandas",
        ]:
            if kw in text:
                skills.append(kw.title() if kw != "llm" else "LLM")

        seniority = "mid"
        if any(x in text for x in ("werkstudent", "working student", "working-student")):
            seniority = "working_student"
        elif any(x in text for x in ("internship", "praktikum", "intern ")):
            seniority = "internship"
        elif any(x in text for x in ("thesis", "masterarbeit", "bachelorarbeit")):
            seniority = "thesis"
        elif any(x in text for x in ("trainee", "absolvent", "graduate", "berufseinsteiger")):
            seniority = "trainee_graduate"
        elif any(x in text for x in ("junior", "entry", "fresher", "einsteiger")):
            seniority = "junior" if "junior" in text else "fresher"
        elif any(x in text for x in ("senior", "lead", "principal", "staff")):
            seniority = "senior"

        field = "other_tech"
        if any(x in text for x in ("data engineer", "dateningenieur", "etl", "spark", "airflow", "dbt")):
            field = "data_engineering"
        elif any(x in text for x in ("machine learning", "data scientist", "llm", "nlp", "mlops", "deep learning")):
            field = "ai_ml_data_science"
        elif any(x in text for x in ("data analyst", "datenanalyst", "analytics")):
            field = "data_analytics"
        elif any(x in text for x in ("business intelligence", "power bi", "tableau", " bi ")):
            field = "business_intelligence"
        elif any(x in text for x in ("devops", "kubernetes", "terraform", "sre")):
            field = "cloud_devops"

        visa_stance = "not_mentioned"
        if any(x in text for x in ("visa sponsorship", "sponsorship available", "visa support")):
            visa_stance = "sponsorship_offered"
        elif any(x in text for x in ("eu citizens only", "eu passport", "only eu")):
            visa_stance = "eu_citizens_only"
        elif any(x in text for x in ("work permit required", "existing permit")):
            visa_stance = "existing_permit_required"
        elif "relocation" in text:
            visa_stance = "relocation_support"

        remote_conf = 0.9 if any(x in text for x in ("remote", "home office", "hybrid")) else 0.2
        work_mode = "remote" if "remote" in text else ("hybrid" if "hybrid" in text else "onsite")
        entry = seniority in {
            "internship",
            "working_student",
            "thesis",
            "trainee_graduate",
            "fresher",
            "junior",
        }
        english_ok = any(x in text for x in ("english", "englisch"))
        language = "en" if english_ok and "german" not in text and "deutsch" not in text else (
            "de" if any(x in text for x in ("german", "deutsch")) else "unknown"
        )
        if english_ok and any(x in text for x in ("german", "deutsch")):
            language = "bilingual"

        assert field in FIELD_VALUES
        assert seniority in SENIORITY_VALUES
        assert visa_stance in VISA_VALUES

        payload = {
            "field": field,
            "seniority": seniority,
            "employment_type": seniority if seniority in {"internship", "working_student", "thesis", "fresher"} else (
                "fresher" if entry else ""
            ),
            "skills": skills[:10],
            "visa_stance": visa_stance,
            "evidence_visa": "",
            "languages_required": ["English"] if english_ok else [],
            "english_ok": english_ok,
            "entry_level": entry,
            "job_seeker_visa_friendly": visa_stance in {"sponsorship_offered", "relocation_support"},
            "is_tech": field != "non_tech",
            "experience_years_min": None,
            "confidence": 0.55,
            "summary": self._summarize(prompt)[:200],
            "work_mode": work_mode,
            "language": language,
            "remote_confidence": remote_conf,
            "salary_min": None,
            "salary_max": None,
            "salary_unit": None,
        }
        return json.dumps(payload)

from typing import Annotated, Any, List

from pydantic import StringConstraints, TypeAdapter
from typing_extensions import TypedDict


class ArbeitnowJob(TypedDict):
    slug: str
    company_name: Annotated[str, StringConstraints(strip_whitespace=True)]
    title: Annotated[str, StringConstraints(strip_whitespace=True)]
    description: str
    remote: bool
    url: str
    tags: List[str]
    job_types: List[str]
    location: str
    created_at: int  # Unix timestamp usually returned by this API


class ArbeitnowResponse(TypedDict):
    data: List[ArbeitnowJob]
    links: dict
    meta: dict


def _coerce_str_list(value: Any) -> List[str]:
    """Arbeitnow occasionally returns tags/job_types as a dict keyed by index."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if v is not None and str(v).strip()]
    if isinstance(value, dict):
        return [str(v) for v in value.values() if v is not None and str(v).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def validate_api_response(raw_json: dict) -> ArbeitnowResponse:
    """
    Uses the vendored TypeAdapter to validate raw API data.
    """
    payload = dict(raw_json or {})
    jobs = []
    for job in payload.get("data") or []:
        if not isinstance(job, dict):
            continue
        normalized = dict(job)
        normalized["tags"] = _coerce_str_list(normalized.get("tags"))
        normalized["job_types"] = _coerce_str_list(normalized.get("job_types"))
        jobs.append(normalized)
    payload["data"] = jobs
    adapter = TypeAdapter(ArbeitnowResponse)
    return adapter.validate_python(payload)

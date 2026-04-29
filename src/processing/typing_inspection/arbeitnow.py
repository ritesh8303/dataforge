from typing import Annotated, List
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
    created_at: int # Unix timestamp usually returned by this API

class ArbeitnowResponse(TypedDict):
    data: List[ArbeitnowJob]
    links: dict
    meta: dict

def validate_api_response(raw_json: dict) -> ArbeitnowResponse:
    """
    Uses the vendored TypeAdapter to validate raw API data.
    """
    adapter = TypeAdapter(ArbeitnowResponse)
    return adapter.validate_python(raw_json)
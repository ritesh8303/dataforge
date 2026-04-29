from typing import Annotated, List
from pydantic import StringConstraints, TypeAdapter
from typing_extensions import TypedDict

class BAJobLocation(TypedDict):
    zip_code: str
    city: str
    state: str

class BAJob(TypedDict):
    refnr: str
    titel: Annotated[str, StringConstraints(strip_whitespace=True)]
    arbeitgeber: str
    eintrittsdatum: str
    modifikationsdatum: str
    arbeitsort: BAJobLocation

class BAResponse(TypedDict):
    stellenangebote: List[BAJob]
    max_results: int
    current_page: int

def validate_ba_response(raw_json: dict) -> BAResponse:
    """
    Uses the validated TypeAdapter to validate BA API raw data.
    """
    adapter = TypeAdapter(BAResponse)
    return adapter.validate_python(raw_json)
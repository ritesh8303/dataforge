import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pydantic import TypeAdapter, PositiveInt, StringConstraints
from typing import Annotated
from typing_extensions import TypedDict

class JobPosting(TypedDict):
    id: PositiveInt
    title: Annotated[str, StringConstraints(strip_whitespace=True, min_length=3)]
    company: str
    remote: bool

def test_validation_logic():
    adapter = TypeAdapter(JobPosting)
    raw_data = {
        "id": 12345,
        "title": "  Data Engineer  ",
        "company": "DataForge Tech",
        "remote": True
    }
    validated = adapter.validate_python(raw_data)
    assert validated["id"] == 12345
    assert validated["title"] == "Data Engineer"

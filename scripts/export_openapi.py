"""Export OpenAPI schema for the Match FastAPI app into the repo."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Import FastAPI stack from site-packages BEFORE adding vendored src/ deps to path.
import fastapi  # noqa: F401
import mangum  # noqa: F401
import pydantic  # noqa: F401

sys.path.insert(0, str(ROOT / "src"))

from api.app import app  # noqa: E402

out = ROOT / "docs" / "openapi.match.json"
out.write_text(json.dumps(app.openapi(), indent=2), encoding="utf-8")
print(f"Wrote {out}")

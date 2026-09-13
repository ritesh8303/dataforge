"""Local Match API (FastAPI + uvicorn) on port 8001.

Serves /match, /jobs, /docs, /openapi.json against local gold CSVs.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from paths import GOLD_DIR, SRC_DIR

# Prefer site-packages FastAPI/Pydantic over vendored Lambda copies under src/.
import fastapi  # noqa: F401
import pydantic  # noqa: F401

sys.path.insert(0, str(SRC_DIR))

os.environ.setdefault("GOLD_BUCKET", "local-mock")
os.environ.setdefault("GOLD_KEY", "all_jobs.csv")
os.environ.setdefault("AI_ENABLED", "true")
# Leave MATCH_API_KEY unset locally so browser demos work without a key.

import boto3


def mock_get_object(Bucket, Key):
    filepath = GOLD_DIR / Key
    if not filepath.exists():
        filepath = Path(__file__).resolve().parents[1] / "data" / "gold" / Key
    with open(filepath, encoding="utf-8") as f:
        content = f.read()
    mock_body = MagicMock()
    mock_body.read.return_value = content.encode("utf-8")
    return {"Body": mock_body}


mock_s3 = MagicMock()
mock_s3.get_object = mock_get_object
boto3.client = lambda service, *args, **kwargs: mock_s3 if service == "s3" else MagicMock()

from api.app import app  # noqa: E402


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("MATCH_PORT", "8001"))
    print(f"DataForge Match API → http://127.0.0.1:{port}/docs")
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")

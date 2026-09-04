"""Fail CI if Gold quality aggregates violate the contract.

Reads data/gold/data_quality_report.csv and writes data/gold/quality_report.json.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from processing.data_quality import evaluate_gold_quality_row  # noqa: E402

CSV_PATH = ROOT / "data" / "gold" / "data_quality_report.csv"
JSON_PATH = ROOT / "data" / "gold" / "quality_report.json"


def load_latest_row(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing quality CSV: {path}")
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError("data_quality_report.csv is empty")
    return df.iloc[-1].to_dict()


def _jsonable(value):
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, AttributeError):
            pass
    if isinstance(value, (bool, int, float)):
        return value
    return str(value)


def main() -> int:
    row = load_latest_row(CSV_PATH)
    failures = evaluate_gold_quality_row(row)
    payload = {
        "source": str(CSV_PATH.relative_to(ROOT)).replace("\\", "/"),
        "row": {k: _jsonable(v) for k, v in row.items()},
        "gate_passed": not failures,
        "failures": failures,
    }
    JSON_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if failures:
        print("Quality gate FAILED:")
        for item in failures:
            print(f"  - {item}")
        return 1
    print(f"Quality gate passed ({payload['row'].get('total_jobs')} jobs). Wrote {JSON_PATH.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

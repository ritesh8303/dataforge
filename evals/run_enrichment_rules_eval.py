"""Evaluate rule-based enrichment classifiers against hand labels."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from enrichment.rules_de_en import classify_job
from evals.metrics import classification_report, confusion_pairs


def main() -> int:
    labels = json.loads(
        (ROOT / "evals" / "data" / "enrichment_labels.json").read_text(encoding="utf-8")
    )
    if len(labels) < 100:
        raise SystemExit(f"Need ≥100 enrichment labels, found {len(labels)}")

    y_field_t, y_field_p = [], []
    y_sen_t, y_sen_p = [], []
    y_visa_t, y_visa_p = [], []

    for row in labels:
        pred = classify_job(row.get("title", ""), row.get("description", ""), row.get("tags", ""))
        y_field_t.append(row["field"])
        y_field_p.append(pred["field"])
        y_sen_t.append(row["seniority"])
        y_sen_p.append(pred["seniority"])
        y_visa_t.append(row["visa_stance"])
        y_visa_p.append(pred["visa_stance"])

    report = {
        "n": len(labels),
        "field": classification_report(y_field_t, y_field_p),
        "seniority": classification_report(y_sen_t, y_sen_p),
        "visa_stance": classification_report(y_visa_t, y_visa_p),
        "field_confusion": confusion_pairs(y_field_t, y_field_p),
    }

    out = ROOT / "evals" / "results" / "enrichment_rules_eval.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "field_macro_f1": report["field"]["macro_f1"],
                "seniority_macro_f1": report["seniority"]["macro_f1"],
                "visa_macro_f1": report["visa_stance"]["macro_f1"],
            },
            indent=2,
        )
    )
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

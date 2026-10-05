# Evaluation harness for thesis experiments

Run from repo root:

```bash
python -m evals.run_matching_eval
python -m evals.run_enrichment_eval
python -m evals.run_router_eval
python -m evals.run_roi_model
```

Outputs land in `evals/results/` for thesis figures.

## Honesty note

CI matching metrics use **synthetic** fixtures (`evals/generate_phase_a_fixtures.py`). Absolute nDCG is not externally calibrated.

For product trust claims, follow `evals/data/human_label_protocol.md` and populate `evals/data/human_labels.csv` (see `.template`). Until then, UI/API copy must describe Match as a **suggested shortlist**, not calibrated hiring intelligence.

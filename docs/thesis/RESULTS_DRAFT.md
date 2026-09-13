# Thesis results draft (RQ1–RQ4) — living

**Programme:** M.Sc. Data Science, University of Europe for Applied Sciences, Potsdam  
**Artefact root:** this repository. Numbers below are reproducible via `evals/` + `scripts/` unless marked *pending live Bedrock*.

## RQ1 — Integrate GenAI without breaking lineage / cost bounds

- Enrichment is **additive** (Silver/Gold contracts unchanged; AI columns appended).
- Kill switch: `AI_ENABLED` + daily budget in `ModelRouter`.
- Lineage: Bronze → Silver SCD2 → Gold; optional Express SFN Silver→Gold (`terraform/stepfunctions.tf`).
- Evidence: `docs/RESPONSIBLE_AI.md`, `docs/BUDGET_RUNBOOK.md`, CI quality gate.

## RQ2 — Provider / rules trade-offs

- Rules baseline: `evals/run_enrichment_rules_eval.py` + `evals/run_rq2_pareto.py` (CI: rules point).
- Live Bedrock attempt (2026-09-13): `eu.amazon.nova-micro-v1:0` via inference profile.
  - Titan Text Express is **EOL** on this account.
  - Nova Micro invoke hit **ThrottlingException: Too many tokens per day** (account free-tier / low quota).
  - Enrichment schedule paused (`AI_ENABLED=false`, `enable_schedule=false`) to free quota.
  - Re-run: `AWS_PROFILE=dataforge-germany BEDROCK_COMPLETION_MODEL=eu.amazon.nova-micro-v1:0 py -3 evals/run_rq2_pareto.py --live-providers --provider bedrock --limit 40`
- Residency: Bedrock `eu-central-1` preferred for EU processing narrative.

## RQ3 — Hybrid match vs rule wizard

From `evals/results/eval_report.md` (42 queries × 96 jobs):

| Method | nDCG@10 | P@5 | Recall@20 | MRR |
|--------|--------:|----:|----------:|----:|
| bm25 | 0.1452 | 0.1619 | 0.2143 | 0.1844 |
| dense | 0.2203 | 0.2190 | 0.2738 | 0.2677 |
| hybrid | 0.1714 | 0.1714 | 0.2341 | 0.2344 |
| heuristic | 0.1585 | 0.1905 | 0.2401 | 0.2117 |

Dense leads on this gold set; hybrid remains the product default (lexical + dense + citations).

## RQ4 — Unit economics

- Scenario model: `scripts/roi_report.py` → `evals/results/roi_report.json`.
- Live costs: flush `CostLogger` JSON from Lambda / local runs into the same script via `--records`.

## Thesis-plus — multi-agent ablation

`evals/results/agent_ablation.json`: local-provider citation *structure* validity ≈ 1.0 for hybrid and multi-agent (saturated). Bedrock faithfulness spot-check still required for the appendix.

## Ethics

Obligation → control table: `docs/RESPONSIBLE_AI.md`. Visa signals advisory + evidence-cited.

## Status honesty

Lakehouse APIs + Pages + **Match Function URL** are live on EU (`366945363779`). Match requires `X-API-Key` (see gitignored `aws-keys-do-not-commit.txt`). Enrichment Lambda schedule is **paused** until Bedrock daily token quota recovers. Multi-agent works via `method=agent` on the same URL (with API key).

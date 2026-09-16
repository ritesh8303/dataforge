# Thesis results draft (RQ1–RQ4) — living

**Programme:** M.Sc. Data Science, University of Europe for Applied Sciences, Potsdam  
**Artefact root:** this repository.  
**Last status check:** 2026-09-16

Numbers below are reproducible via `evals/` + `scripts/` unless marked *pending AWS Bedrock quota*.

## Snapshot

| RQ | Status | Artefact |
|----|--------|----------|
| RQ1 | **Done (code + AWS)** | Additive AI schema, kill switch, SFN Express, budget runbook |
| RQ2 | **Partial** — rules baseline done; live Nova Micro blocked on account quota = 0 (increase requested) | `evals/run_rq2_pareto.py`, Service Quotas request pending |
| RQ3 | **Done (labelled eval)** | `evals/results/eval_report.md` |
| RQ4 | **Done (modelled)** — live CostLogger flush optional | `evals/results/roi_report.json` |
| Plus | **Partial** — structural ablation done; Bedrock faithfulness pending quota | `evals/results/agent_ablation.json` |

## RQ1 — Integrate GenAI without breaking lineage / cost bounds

- Enrichment is **additive** (Silver/Gold contracts unchanged; AI columns appended).
- Kill switch: `AI_ENABLED` + daily budget in `ModelRouter`.
- Lineage: Bronze → Silver SCD2 → Gold; Express SFN Silver→Gold (`terraform/stepfunctions.tf`).
- Match Function URL live; enrichment schedule **paused** while Nova Micro account quota is 0.
- Evidence: `docs/RESPONSIBLE_AI.md`, `docs/BUDGET_RUNBOOK.md`, CI quality gate.

## RQ2 — Provider / rules trade-offs

**Rules baseline (available now):** `evals/run_enrichment_rules_eval.py` + `evals/run_rq2_pareto.py` (no LLM).

**Live Bedrock (blocked → increase requested):**
- Target model: `eu.amazon.nova-micro-v1:0` (inference profile). Titan Text Express is EOL.
- Account applied quotas for Nova Micro were **0 TPM / 0 TPD** (AWS default TPM is higher; account locked at zero).
- Quota increase requested 2026-09-16: Cross-region Nova Micro TPM → 1,000,000 (`L-DC7FF66C`, status PENDING).
- After approval, re-run:
  ```bash
  AWS_PROFILE=dataforge-germany BEDROCK_COMPLETION_MODEL=eu.amazon.nova-micro-v1:0 AI_ENABLED=true \
    py -3 evals/run_rq2_pareto.py --live-providers --provider bedrock --limit 40
  ```
- Residency narrative: Bedrock `eu-central-1` (no OpenAI/Anthropic bypass for this thesis track).

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

- Scenario model: `scripts/roi_report.py` → `evals/results/roi_report.json` (Nova Micro + Titan Embed order-of-magnitude; rules-first enrich).
- Live costs: flush `CostLogger` JSON from Lambda / local runs via `--records` after Bedrock quota is usable.

## Thesis-plus — multi-agent ablation

`evals/results/agent_ablation.json`: local-provider citation *structure* validity ≈ 1.0 for hybrid and multi-agent (saturated). Bedrock faithfulness spot-check still required for the appendix after quota approval.

## Ethics

Obligation → control table: `docs/RESPONSIBLE_AI.md`. Visa signals advisory + evidence-cited. Match requires API key in production.

## Status honesty (portfolio claim)

| Claim | OK to say? |
|-------|------------|
| Live medallion lakehouse + Jobs/Metrics APIs + Pages | Yes |
| Live Match Function URL (hybrid, API key) | Yes |
| Multi-agent / MCP in repo + callable with key | Yes |
| Enrichment running nightly on Bedrock | **No** — paused until quota |
| Live Bedrock RQ2 Pareto table | **No** — pending quota approval |

Demo talk-track: `docs/DEMO_SCRIPT.md`.

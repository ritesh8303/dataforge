# Thesis results draft (RQ1–RQ4) — living

**Programme:** M.Sc. Data Science, University of Europe for Applied Sciences, Potsdam  
**Artefact root:** this repository.  
**Last status check:** 2026-09-30

Numbers below are reproducible via `evals/` + `scripts/` unless marked *pending*.

## Snapshot

| RQ | Status | Artefact |
|----|--------|----------|
| RQ1 | **Done (code + AWS)** | Additive AI schema, kill switch, SFN Express, budget runbook |
| RQ2 | **Partial (live)** — rules 40/40; OpenAI gpt-4o-mini **16/40** ($0.00044) under RPM 429; Bedrock still blocked | `evals/results/rq2_pareto.json` |
| RQ3 | **Done (labelled eval)** | `evals/results/eval_report.md`, `evals/results/error_analysis.md` |
| RQ4 | **Done (modelled)** — live CostLogger flush optional | `evals/results/roi_report.json` |
| Plus | **Partial** — structural ablation done; Bedrock faithfulness pending quota | `evals/results/agent_ablation.json` |

## RQ1 — Integrate GenAI without breaking lineage / cost bounds

- Enrichment is **additive** (Silver/Gold contracts unchanged; AI columns appended).
- Kill switch: `AI_ENABLED` + daily budget in `ModelRouter`.
- Lineage: Bronze → Silver SCD2 → Gold; Express SFN Silver→Gold (`terraform/stepfunctions.tf`).
- Match Function URL live; enrichment schedule **paused** while Nova Micro account quota is 0.
- Match loads vectors via `VECTOR_STORE_URI` (LanceDB when installed) with `embedding_index.json` fallback.
- Evidence: `docs/RESPONSIBLE_AI.md`, `docs/BUDGET_RUNBOOK.md`, CI quality gate.

## RQ2 — Provider / rules trade-offs

**Rules baseline (available now):** `evals/run_enrichment_rules_eval.py` + `evals/run_rq2_pareto.py` (no LLM).

**Live Bedrock (blocked — checked 2026-09-30):**
- Target model: `eu.amazon.nova-micro-v1:0`. Applied quota still **0** TPM/TPD.
- Case `178955627400906` (`L-DC7FF66C`) status **`CASE_OPENED`** since 2026-09-16 (no update).
- Account-wide Bedrock completion quotas are zero — switching Nova→Claude on Bedrock does not help.

**Live OpenAI gpt-4o-mini (2026-09-30) — partial success under new-account RPM:**

| Metric | Rules (n=40) | OpenAI gpt-4o-mini |
|--------|-------------:|-------------------:|
| n_ok / n | 40 / 40 | **16 / 40** |
| avg latency | 0.16 ms | **1356 ms** (ok calls) |
| p50 latency | 0.13 ms | 1288 ms |
| total cost | $0 | **$0.00044** (~€0.0004) |
| avg cost / job | $0 | $0.000011 |

- Artefact: `evals/results/rq2_pareto.json` (strict pin; local fallback disabled).
- Failures were **HTTP 429** — first RPM, then **daily RPD exhausted** (`Used 10000 / Limit 10000`, reset ~**24h**). Aggressive retries during debugging burned the day quota.
- **To get all 40/40:** wait until the RPD window resets (check [rate limits](https://platform.openai.com/account/rate-limits)), then run **once** with modest pacing (no retry storm):

```powershell
$env:AI_ENABLED = "true"
$env:OPENAI_API_KEY = "<key set locally — do not paste in chat>"
$env:OPENAI_COMPLETION_MODEL = "gpt-4o-mini"
py -3 -u evals/run_rq2_pareto.py --live-providers --provider openai --limit 40 --sleep 2
```

After Bedrock approval:
```bash
AWS_PROFILE=dataforge-germany BEDROCK_COMPLETION_MODEL=eu.amazon.nova-micro-v1:0 AI_ENABLED=true \
  py -3 evals/run_rq2_pareto.py --live-providers --provider bedrock --limit 40
```

## RQ3 — Hybrid match vs rule wizard

From `evals/results/matching_eval.json` (**103 queries × 96 jobs**, expanded gold set 2026-09-18).  
Qualitative error analysis: [`evals/results/error_analysis.md`](../evals/results/error_analysis.md).  
Figures: `docs/thesis/latex/figures/rq3_comparison.svg`, `rq3_bars.png`.

| Method | nDCG@10 | P@5 | Recall@20 | MRR |
|--------|--------:|----:|----------:|----:|
| bm25 | 0.1351 | 0.1437 | 0.1926 | 0.1906 |
| dense | **0.2174** | **0.2175** | **0.2856** | **0.2706** |
| hybrid | 0.1668 | 0.1670 | 0.2225 | 0.2327 |
| heuristic | 0.1462 | 0.1728 | 0.2071 | 0.1952 |

Dense leads on all metrics on this expanded set; hybrid remains the product default (lexical + dense + citations). Prior 42-query baseline: dense nDCG@10 = 0.2203 (stable after scaling query count).

## RQ4 — Unit economics

- Scenario model: `scripts/roi_report.py` → `evals/results/roi_report.json` (Nova Micro + Titan Embed order-of-magnitude; rules-first enrich) ≈ **€0.06 / 1k jobs**.
- Live costs: flush `CostLogger` JSON from Lambda / local runs via `--records` after a live provider run.

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
| LanceDB vector path with JSON fallback | Yes (code + tf env; confirm backend in Match response `vector_backend`) |
| Enrichment running nightly on Bedrock | **No** — paused until quota |
| Live Bedrock RQ2 Pareto table | **No** — case still open |
| Live OpenAI RQ2 cells | **Yes (partial)** — 16/40 gpt-4o-mini ok; rest 429 — see RESULTS_DRAFT |

Demo talk-track: `docs/DEMO_SCRIPT.md`. Packaging: [`PACKAGING.md`](PACKAGING.md).

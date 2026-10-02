# Thesis results draft (RQ1–RQ4) — living

**Programme:** M.Sc. Data Science, University of Europe for Applied Sciences, Potsdam  
**Artefact root:** this repository.  
**Last status check:** 2026-10-02

Numbers below are reproducible via `evals/` + `scripts/` unless marked *pending*.

## Snapshot

| RQ | Status | Artefact |
|----|--------|----------|
| RQ1 | **Done (code + AWS)** | Additive AI schema, kill switch, SFN Express, budget runbook |
| RQ2 | **Done (live OpenAI)** — rules 40/40; gpt-4o-mini **40/40** (~$0.0011); Bedrock still quota-blocked | `evals/results/rq2_pareto.json` |
| RQ3 | **Done (labelled eval)** | `evals/results/eval_report.md`, `evals/results/error_analysis.md` |
| RQ4 | **Done (modelled + live)** — OpenAI scenario ≈ €0.10/1k; RQ2 live CostLogger flush | `evals/results/roi_report.json`, `docs/roi.html` |
| Plus | **Done (structural)** — faithfulness spot-check on hybrid citations; LLM judge optional | `evals/results/faithfulness_spotcheck.json` |

## RQ1 — Integrate GenAI without breaking lineage / cost bounds

- Enrichment is **additive** (Silver/Gold contracts unchanged; AI columns appended).
- Kill switch: `AI_ENABLED` + daily budget in `ModelRouter`.
- Lineage: Bronze → Silver SCD2 → Gold; Express SFN Silver→Gold (`terraform/stepfunctions.tf`).
- Match Function URL live; enrichment schedule **enabled** (`cron(30 21 * * ? *)` UTC) with OpenAI-first sample rate 0.25.
- Live enrichment (2026-10-01): **304** product-board jobs → **28** LLM enrichment rows + embedding index (vector backend `json` fallback when LanceDB wheel absent in Lambda).
- Match loads vectors via `VECTOR_STORE_URI` (LanceDB when installed) with `embedding_index.json` fallback.
- Evidence: `docs/RESPONSIBLE_AI.md`, `docs/BUDGET_RUNBOOK.md`, CI quality gate.

## RQ2 — Provider / rules trade-offs

**Rules baseline:** `evals/run_enrichment_rules_eval.py` + `evals/run_rq2_pareto.py` (no LLM).

**Live OpenAI gpt-4o-mini (2026-10-01) — complete:**

| Metric | Rules (n=40) | OpenAI gpt-4o-mini |
|--------|-------------:|-------------------:|
| n_ok / n | 40 / 40 | **40 / 40** |
| avg latency | ~0.18 ms | **~1125 ms** |
| total cost | $0 | **~$0.0011** |
| provider | rules | openai |

- Artefacts: `evals/results/rq2_pareto.json` (strict pin; paced `--sleep 2`); console log `evals/results/rq2_run.log`.
- Production router is **OpenAI-first** while Bedrock Nova Micro quota remains blocked.

**Live Bedrock (still blocked):**
- Target model: `eu.amazon.nova-micro-v1:0`. Applied quota still **0** TPM/TPD unless newly approved.
- Case `178955627400906` (`L-DC7FF66C`) was `CASE_OPENED` since 2026-09-16.
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

**CI gate note (2026-10-01):** `evals/run_matching_eval.py` now **pins local-tfidf embeddings** so CI is offline-deterministic. Re-measure dense nDCG@10 ≈ **0.1258** (`evals/baselines/ndcg_baseline.json`). The packaging figure **0.2174** above is retained as the thesis table; investigate provider drift separately if republishing RQ3.

## RQ4 — Unit economics

- Scenario model (OpenAI-first production path): `scripts/roi_report.py` → `evals/results/roi_report.json` ≈ **€0.10 / 1k jobs** (gpt-4o-mini enrich @ 30% sample + text-embedding-3-small). Nova/Titan remain cheaper once Bedrock quota opens.
- Live CostLogger (RQ2 OpenAI 40 enrich calls): **~$0.0011** total → ≈ **€0.025 / 1k jobs** proxy (enrich-only; see `docs/roi.html`).
- Page: [`docs/roi.html`](../roi.html).

## Thesis-plus — multi-agent ablation + faithfulness

`evals/results/agent_ablation.json`: local-provider citation *structure* validity ≈ 1.0 for hybrid and multi-agent (saturated).

`evals/results/faithfulness_spotcheck.json` (2026-10-02): hybrid citation structural spot-check via `evals/run_faithfulness_spotcheck.py` (**20** citations in the committed artefact; CI gate uses `--limit 10`). Optional `--use-llm-judge` with OpenAI when budget allows.

## Ethics

Obligation → control table: `docs/RESPONSIBLE_AI.md`. Visa signals advisory + evidence-cited. Match requires API key in production.

## Status honesty (portfolio claim)

| Claim | OK to say? |
|-------|------------|
| Live medallion lakehouse + Jobs/Metrics APIs + Pages | Yes |
| Live Match Function URL (hybrid, API key) | Yes |
| Nightly **OpenAI** enrichment (sample rate 0.25) | Yes — 304 product-board jobs → 28 LLM rows (2026-10-01) |
| Multi-agent / MCP in repo + callable with key | Yes |
| LanceDB vector path with JSON fallback | Yes (code + tf env; confirm backend in Match response `vector_backend`) |
| Enrichment running nightly on **Bedrock** | **No** — Nova Micro quota still 0 |
| Live Bedrock RQ2 Pareto table | **No** — case `178955627400906` still open |
| Live OpenAI RQ2 cells | **Yes** — gpt-4o-mini **40/40**, ~$0.0011, ~1125 ms avg |

Demo talk-track: `docs/DEMO_SCRIPT.md`. Packaging: [`PACKAGING.md`](PACKAGING.md).

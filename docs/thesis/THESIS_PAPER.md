# Content ledger (superseded as the official outline)

**The binding thesis is** [`latex/overleaf_main.tex`](latex/overleaf_main.tex). **Current measured numbers:** [`RESULTS_DRAFT.md`](RESULTS_DRAFT.md) (last check 2026-10-02).

This Markdown file keeps an older ARM-style chapter order. Do not submit this file as the thesis.

**Working title (UE):** Augmented Analytics on a Production Lakehouse: Integrating Generative AI into European Job Intelligence — A Case Study on DataForge

**Programme:** M.Sc. Data Science (2-year)  
**Institution:** University of Europe for Applied Sciences, Potsdam  
**Author:** Ritesh Rakesh Jadhav  
**Date:** October 2026  
**Reference implementation:** https://github.com/ritesh8303/dataforge

---

## Abstract

European job postings are fragmented across public employment services, aggregators, and applicant-tracking systems. DataForge is a production serverless medallion lakehouse on Amazon Web Services (AWS) that already ingests multi-source vacancy data into Bronze, Silver (SCD Type 2), and Gold layers with public Jobs/Metrics APIs. Industry demand, however, has shifted from raw aggregation toward generative-AI (GenAI) features such as semantic matching and structured enrichment—features that risk breaking lineage, exploding cost, and creating non-reproducible “AI theatre” if bolted on without measurement.

This thesis designs, implements, and evaluates a **frugal GenAI integration layer** on the existing DataForge system: a multi-provider model gateway with kill switches and budgets; rules-first enrichment with **OpenAI-first** production (`gpt-4o-mini`) while Bedrock quota is blocked; hybrid BM25 + dense retrieval for Match; FastAPI Match API with PII redaction and cited explanations; optional bounded multi-agent orchestration and an MCP tool bridge. Four research questions address (RQ1) non-destructive integration under cost envelopes, (RQ2) provider trade-offs under residency and budget constraints, (RQ3) ranked relevance versus a rule-based Career Matching Wizard, and (RQ4) unit economics of the AI layer.

On an expanded labelled matching set (103 queries × 96 jobs), dense retrieval achieves the highest nDCG@10 (**0.217**) versus heuristic (**0.146**) and BM25 (**0.135**) in the packaging semantic-embed run. Hybrid remains the product default. CI later pins local tf-idf embeddings (dense nDCG@10 **0.126**); both numbers are reported. Modelled OpenAI-first unit economics on a 1,000-job scenario are about **€0.10**. Live RQ2 on OpenAI gpt-4o-mini completed **40/40** jobs (~$0.0011). Live Bedrock Nova Micro remains **pending AWS quota**. Nightly OpenAI enrichment is on (sample 0.25).

**Keywords:** medallion architecture; LLMOps; hybrid retrieval; job matching; unit economics; responsible AI; AWS Bedrock; European labour-market data.

---

## Table of contents

1. Introduction and industry problem  
2. Related work  
3. DataForge as existing system  
4. Requirements and design of the AI integration layer  
5. Implementation  
6. Evaluation results  
7. Business / ROI analysis  
8. Discussion, ethics, limitations  
9. Conclusion and future work  
References  
Appendices  

---

## 1. Introduction and industry problem

### 1.1 Motivation

Labour-market information in Germany and the wider EU is split across federal portals (e.g. Bundesagentur für Arbeit Jobsuche), EURES, commercial aggregators, and company career pages. For **interns, working students, and juniors**—especially candidates navigating visa and language constraints—search costs are high: seniority wording is inconsistent, English-OK signals are buried in prose, and visa stance is often unstated.

DataForge was built as a portfolio and applied-research substrate to address fragmentation with classical data engineering: validated ingest, medallion storage, SCD Type 2 history, analytics Gold, and public APIs. In parallel, employer demand for applied GenAI skills (RAG, agents, LLMOps, cost governance) has intensified. The scientific and engineering problem is therefore not “build a chatbot,” but:

> How can GenAI be added to an **already live** medallion pipeline so that lineage, reproducibility, cost, and compliance remain intact—and so that claims are **falsifiable**?

### 1.2 Problem statement

Three failure modes dominate industry GenAI add-ons:

1. **Lineage breakage** — LLM outputs overwrite source-of-truth keys or SCD history.  
2. **Unmeasured quality** — demos without held-out labels or saturated metrics (e.g. nDCG = 1.0 on toy sets).  
3. **Unbounded cost** — frontier models on full corpora without budgets, sample rates, or kill switches.

This thesis treats DataForge as a **case study** of controlled GenAI integration under a hard operational budget (target €0–25/month typical; €40 thesis hard cap) in `eu-central-1`.

### 1.3 Research questions and hypotheses

| ID | Research question |
|----|-------------------|
| **RQ1** | How can GenAI be integrated into Bronze→Silver→Gold→API without breaking SCD Type 2 semantics, reproducibility, or free-tier / low-budget cost envelopes? |
| **RQ2** | For enrichment and related tasks, which provider/model class wins on quality, cost, latency, and EU data residency? |
| **RQ3** | Do embeddings + hybrid retrieval outperform the 60/20/20 rule-based Career Matching Wizard on ranked job relevance? |
| **RQ4** | Under which packaging does AI-on-pipeline create favourable unit economics versus pure open-data aggregation? |

**Hypotheses.**  
**H1:** Dense / hybrid retrieval improves nDCG@10 relative to the heuristic wizard on a held-out graded query set.  
**H2:** Cheap batch / small models (rules-first + Nova-class) suffice for enrichment relative to frontier models for this domain.  
**H3:** A task-aware router with budgets reduces spend at comparable quality versus unconstrained single-provider use.  
**H4:** Match/enrichment features have better €-per-value economics than selling raw CSVs alone.

### 1.4 Contributions

1. A **reproducible AI integration layer** (gateway, enrichment, Match API, evals, governance docs) on a live AWS lakehouse.  
2. An **honest evaluation protocol** (matching metrics, rules F1, ROI model, ablation hooks) with CI gates against saturated scores.  
3. A **responsible-AI mapping** for advisory job matching (EU AI Act / GDPR framing; visa sensitivity).  
4. An applied **unit-economics** analysis tied to pricing assumptions and CostLogger infrastructure.

### 1.5 Thesis structure

Chapter 2 reviews related work. Chapter 3 describes the pre-existing DataForge system. Chapter 4 states requirements and design. Chapter 5 details implementation. Chapter 6 reports evaluation. Chapter 7 analyses ROI. Chapter 8 discusses ethics and limitations. Chapter 9 concludes.

---

## 2. Related work

### 2.1 Medallion / lakehouse architectures

Medallion architectures (Bronze/Silver/Gold) organise progressive refinement of data products (Armbrust et al., 2021; Databricks, 2020). SCD Type 2 patterns preserve history for slowly changing dimensions (Kimball & Ross, 2013). This thesis does **not** invent medallion design; it studies GenAI as an **additive stage** atop an existing SCD2 Silver and Gold contract.

### 2.2 Neural and hybrid retrieval

Dense retrieval with bi-encoders (Karpukhin et al., 2020) and hybrid fusion with BM25 via reciprocal rank fusion (Cormack et al., 2009; formalised in later IR practice) underpin modern RAG stacks. Evaluation uses graded relevance metrics such as nDCG (Järvelin & Kekäläinen, 2002), MRR, Precision@k, and Recall@k. Job matching is a specialised IR task with noisy multilingual text and sparse structured fields.

### 2.3 LLMOps and multi-provider routing

Production LLM systems require prompt versioning, observability, fallback cascades, and cost controls (often summarised as LLMOps). Multi-provider routing reflects EU residency preferences (e.g. Bedrock in-region) versus US SaaS APIs. Empirical “LLM-as-a-service” comparisons must report latency percentiles and € per 1k tokens, not only qualitative demos.

### 2.4 Agents and tool use

Tool-using agents (e.g. ReAct-style loops; Yao et al., 2023) and graph-orchestrated specialists can improve structured outputs when bounded. Unbounded agent swarms are out of scope for cost and controllability. This work uses a **bounded** Supervisor→Retriever→Scorer→Explainer→Critic graph (max 4 LLM calls / 6 handoffs) and a thin MCP bridge for IDE tooling.

### 2.5 Responsible AI in employment contexts

The EU AI Act treats certain employment-related AI as high-risk when used for hiring decisions. Advisory job-seeker ranking of **public vacancies** is a different deployment mode and must not be conflated with employer screening. GDPR principles (purpose limitation, minimisation, storage limitation) constrain resume handling. Related policy sources: GDPR (EU, 2016); AI Act (EU, 2024).

### 2.6 Research gap

Few applied MSc studies combine (a) a **live multi-source medallion system**, (b) **guardrailed GenAI integration**, (c) **falsifiable matching experiments with a non-AI baseline**, and (d) a **unit-economics chapter** grounded in pricing models and router logs. DataForge supplies the production substrate for that combination.

---

## 3. DataForge as existing system

### 3.1 Scope of the baseline system

Before the thesis AI layer, DataForge already provided:

| Component | Role |
|-----------|------|
| Ingest Lambdas + GitHub Actions | Arbeitnow, BA, ATS/Personio, Berlin startups, EURES, later Himalayas/HN |
| Bronze S3 | Raw Parquet, short retention |
| Silver S3 | SCD Type 2 job history |
| Gold S3 + committed aggregates | Analytics CSVs, metrics, dbt/DuckDB path |
| API Gateway | Jobs search + Metrics |
| GitHub Pages | Dashboard, job board, Career Matching Wizard (heuristic) |

Region: **`eu-central-1`**. Infrastructure as code: Terraform with S3 state backend.

### 3.2 Layer contracts (non-negotiable for RQ1)

- **Bronze** may not invent business KPIs.  
- **Silver** owns identity and SCD validity; GenAI must not mutate `job_id` / SCD keys.  
- **Gold** may consume additive enrichment joins.  
- Quality gates (Pydantic ingest, region taxonomy, CI quality script, dbt tests) remain authoritative.

### 3.3 Heuristic Career Matching Wizard (baseline for RQ3)

The pre-AI wizard scores candidates with a **60% skills / 20% seniority / 20% location** heuristic. It is an honest non-neural baseline—useful scientifically precisely because it is simple and transparent.

### 3.4 Audience refinement for the thesis product

The Match surface targets **intern / working-student / junior** seekers with optional visa-aware filters (e.g. Chancenkarte / job-seeker friendly heuristics). Visa fields are **advisory**, evidence-cited, and never legal advice.

---

## 4. Requirements and design of the AI integration layer

### 4.1 Functional requirements

| ID | Requirement |
|----|-------------|
| F1 | Rules-first DE/EN classification for field, seniority, visa stance, languages |
| F2 | Optional LLM enrichment only when rules are ambiguous / sample rate allows |
| F3 | Hybrid BM25 + dense retrieval with RRF; optional LanceDB-on-S3 |
| F4 | Match API: FastAPI + Mangum; citations with `job_id` + evidence |
| F5 | PII redaction before LLM calls; API key on production Match |
| F6 | Optional multi-agent graph + HITL queue for low confidence |
| F7 | MCP tools over HTTPS Match/Jobs |
| F8 | Eval harness + CI gates |

### 4.2 Non-functional requirements

| ID | Requirement |
|----|-------------|
| N1 | Hard cost envelope (€40 thesis cap; daily USD budget on router) |
| N2 | Kill switch `AI_ENABLED` |
| N3 | Prefer EU-resident inference (Bedrock `eu-central-1`) |
| N4 | Reproducible fixtures under `evals/` |
| N5 | Honest portfolio claims (no “live AI” without apply) |

### 4.3 Architecture (logical)

```
Public JDs → Ingest → Bronze → Silver (SCD2, is_tech, rules fields)
                              ↓
                         Gold marts + Jobs/Metrics APIs + Pages
                              ↓
              [optional] Enrichment (rules → Bedrock) → ai_enrichment + embeddings
                              ↓
User / MCP → Match Function URL (FastAPI) → hybrid retrieval → citations
                              ↓
                    [optional] multi-agent graph (bounded) → HITL
```

Orchestration proofs: EventBridge + Lambda (production), Step Functions Express Silver→Gold (thin proof), Airflow DAG in Docker (keyword proof, not MWAA).

### 4.4 Design principles

1. **Additive, not invasive** — AI columns join; SCD keys immutable.  
2. **Rules before tokens** — save Bedrock for ambiguity.  
3. **Measure before marketing** — CI evals; non-saturated gold set.  
4. **Bound agents** — fixed handoff/LLM caps.  
5. **Transparency** — citations, disclaimers, OpenAPI.

---

## 5. Implementation

### 5.1 Model gateway (`src/ai_gateway/`)

`ModelRouter` implements task profiles (`enrich`, `embed`, `explain`, …) with an **OpenAI-first** production cascade while Bedrock Nova Micro quota is zero (then Anthropic, then local). Bedrock in `eu-central-1` remains the preferred EU-resident path when quota is approved. JSON validation, CostLogger, daily budget checks, and `AI_ENABLED` kill switch apply to all paths. Prompt versions live under `prompts/` with a registry. Tracing helpers support Langfuse (env-gated) and S3/CloudWatch-oriented spans.

Production enrichment uses **`gpt-4o-mini`**; embeddings use **`text-embedding-3-small`**. Bedrock completion target is **`eu.amazon.nova-micro-v1:0`**; Titan Embed Text v2 when Bedrock quota opens.

### 5.2 Rules-first enrichment (`src/enrichment/`)

`rules_de_en.py` provides deterministic DE/EN classifiers for tech field taxonomy, seniority, visa stance, languages, and derived flags (`entry_level`, English-OK heuristics). The enricher calls LLMs only when configured and when rules mark ambiguity / sample rate permits. Silver/Gold schemas gain additive AI and rule fields without rewriting SCD logic.

### 5.3 Retrieval (`src/retrieval/`, `src/vector_store.py`)

BM25, dense ranking, and reciprocal rank fusion implement hybrid retrieval. LanceDB-on-S3 is supported as an optional persistence path; JSON embedding indices remain the test fallback.

### 5.4 Match API (`src/api/`)

FastAPI application exposes `/health`, `/jobs`, `/match`, `/match/agent`, OpenAPI. Mangum adapts to Lambda. Production deployment uses a **Function URL** (60s timeout) plus optional API Gateway routes. Security: optional `X-API-Key`, CORS, PII redaction (`src/security/pii.py`), markdown Accept mode for agent-readable surfaces.

### 5.5 Multi-agent and MCP

In-process graph: Supervisor → Retriever → Scorer → Explainer → Critic with one explainer retry budget. LangGraph is optional; sequential fallback is default. MCP server (`mcp/server.py`) exposes `search_jobs`, `get_job`, `match_resume` over stdio and auto-loads the Match API key from a gitignored local file when present.

### 5.6 Infrastructure and DevX

- `terraform/ai.tf` — enrichment + Match + Function URL permissions  
- `terraform/stepfunctions.tf` — Express Silver→Gold (schedule off by default)  
- `docker-compose.yml` — analytics, Match API profile, Airflow profile  
- CI — ruff, pytest, terraform validate, dbt run/test/docs, matching + rules evals  

### 5.7 Operational status at draft time (honesty)

| Capability | Status |
|------------|--------|
| Lakehouse + Jobs/Metrics + Pages | Live |
| Match Function URL + API key | Live |
| Nightly OpenAI enrichment (sample 0.25) | **Live** (304 jobs → 28 LLM rows, 2026-10-01) |
| Nightly Bedrock enrichment | **Paused** (Nova Micro quota 0) |
| RQ2 live OpenAI Pareto (`gpt-4o-mini`) | **Complete** (40/40, ~$0.0011) |
| RQ2 live Bedrock Pareto | **Pending** quota approval |
| RQ3 labelled matching eval | Complete (103×96) |
| RQ4 modelled ROI + live CostLogger | Complete |

---

## 6. Evaluation results

### 6.1 Methodology overview

| Experiment | Protocol | Metric |
|------------|----------|--------|
| Matching (RQ3) | 103 graded queries × 96 jobs; BM25 / dense / hybrid / heuristic | nDCG@10, P@5, Recall@20, MRR |
| Rules enrichment | Labelled jobs for field/seniority/visa/English | Precision / Recall / F1 |
| Agent ablation | 30 queries; hybrid vs multi-agent | Citation structure validity |
| RQ2 Pareto | Rules vs pinned provider (OpenAI complete; Bedrock when quota opens) | Latency, €, success rate |
| ROI model | OpenAI-first pricing × token scenario | € per 1k jobs / per match share |

Fixtures and runners live under `evals/`; results under `evals/results/`.

### 6.2 RQ3 — Matching quality

From `evals/results/matching_eval.json` (packaging semantic-embed run):

| Method | nDCG@10 | P@5 | Recall@20 | MRR |
|--------|--------:|----:|----------:|----:|
| bm25 | 0.1351 | 0.1437 | 0.1926 | 0.1906 |
| **dense** | **0.2174** | **0.2175** | **0.2856** | **0.2706** |
| hybrid | 0.1668 | 0.1670 | 0.2225 | 0.2327 |
| heuristic | 0.1462 | 0.1728 | 0.2071 | 0.1952 |

**Interpretation.** Dense retrieval outperforms the heuristic wizard on nDCG@10 (**H1 supported** on this packaging run). Hybrid trails pure dense on this label distribution but remains the **product default** for lexical fallback and citations. CI later pins local tf-idf embeddings (dense nDCG@10 **0.1258**); both numbers are reported separately.

### 6.3 RQ2 — Provider trade-offs

**Rules baseline:** 40/40 jobs, ~0.18 ms avg latency, €0 model spend.

**Live OpenAI `gpt-4o-mini` (2026-10-01):** 40/40 jobs, ~1125 ms avg latency, **~$0.0011** total (`evals/results/rq2_pareto.json`, strict pin).

**Blocked:** live Nova Micro completions at **0 tokens/minute and 0 tokens/day**. Quota case `178955627400906` was still open. RQ2 is **closed for OpenAI** and **still open for Bedrock**; the runner `evals/run_rq2_pareto.py --live-providers --provider bedrock` is unchanged for post-quota runs.

### 6.4 Multi-agent ablation (thesis-plus)

On the local provider, average citation **structure** validity was 1.0 for both hybrid single-shot and multi-agent (30 queries). This indicates the schema/citation contract is enforced, but **faithfulness** under Bedrock still requires a post-quota spot-check (local saturation does not prove semantic grounding).

### 6.5 Threats to validity

- Label set size (103×96) is adequate for an applied MSc pilot, not an industrial IR benchmark.  
- Dense advantage may partly reflect label construction and embedder choice; CI local tf-idf pin differs from the packaging table.  
- ROI combines modelled OpenAI-first list prices with a live CostLogger flush from RQ2 OpenAI (40 calls).  
- RQ2 Bedrock live cells remain incomplete pending quota.

---

## 7. Business / ROI analysis

### 7.1 Cost model (RQ4)

Scenario: 1,000 jobs with rules-first enrichment (≈30% LLM calls) + embeddings for corpus and 100 match requests (`scripts/roi_report.py`):

| Component | Approx. USD | Approx. EUR (FX 0.92) |
|-----------|------------:|----------------------:|
| Enrich LLM full corpus | 0.330 | 0.304 |
| Enrich rules-first (30%) | 0.099 | 0.091 |
| Embeddings (`text-embedding-3-small`) | 0.009 | 0.008 |
| **Total (rules-first path)** | **0.108** | **0.099** |
| Per enriched job (rules-first) | 9.9e-5 | 9.1e-5 |

**Interpretation.** OpenAI-first rules-first enrichment for a 1k-job batch is about **€0.10**, not tens of euros—supporting **H2/H4** directionally. Live RQ2 CostLogger on 40 enrich calls totals **~$0.0011** (~€0.025/1k jobs enrich-only proxy). Nova/Titan remain cheaper on list price once Bedrock quota opens.

### 7.2 Control levers

| Lever | Effect |
|-------|--------|
| Rules-first + sample rate | Cuts LLM volume |
| Daily budget + kill switch | Hard stop |
| Match API key + API GW throttle | Abuse control |
| Enrichment schedule pause | Emergency quota protection |
| Reserved concurrency | Limited on new accounts (unreserved floor) |

### 7.3 Product packaging implications

Pure CSV dumps of public jobs have weak differentiation. **Cited semantic Match**, visa/entry filters, and quality-gated lakehouse lineage are the monetisable / hireable artefacts—even when absolute € spend is tiny—because they demonstrate controlled GenAI operations, not only scrapers.

---

## 8. Discussion, ethics, limitations

### 8.1 Answers to RQs (current evidence)

| RQ | Answer (draft) |
|----|----------------|
| RQ1 | Achieved via additive schemas, immutable SCD keys, Terraform AI module, budgets/kill switch; nightly **OpenAI** enrichment live (Bedrock path paused for quota). |
| RQ2 | **Closed for OpenAI** (40/40, ~$0.0011). **Bedrock cells pending** quota. Rules baseline established. |
| RQ3 | Dense > heuristic on packaging run (0.217 vs 0.146); H1 supported there; CI tf-idf pin reported separately. |
| RQ4 | Modelled OpenAI-first ≈ **€0.10/1k jobs**; live CostLogger from RQ2 OpenAI run recorded. |

### 8.2 Responsible AI

DataForge Match is an **advisory job-discovery** aid, not an employer hiring decision system. Controls include transparency (citations, disclaimers), human oversight (HITL on low confidence), PII redaction, and sourcing documentation (`docs/RESPONSIBLE_AI.md`, `docs/DATA_SOURCING.md`). Visa heuristics are sensitive and must remain evidence-linked and non-blocking by default.

### 8.3 Limitations

1. Bedrock Nova Micro quota still blocks live Bedrock RQ2; OpenAI RQ2 is complete.  
2. Matching label scale is pilot-sized (103 queries).  
3. Multi-agent faithfulness under real LLMs not yet spot-checked.  
4. Official UE formatting rules must still be applied in the bound version.  
5. Source coverage and `not_mentioned` visa majority limit some product KPIs.

### 8.4 Implications for practice

Applied GenAI theses should treat **quota and billing mechanics** as first-class experimental constraints—equal to model choice. “Architecture complete / measurement pending” is a valid scientific state when documented with a frozen protocol.

---

## 9. Conclusion and future work

### 9.1 Conclusion

This thesis showed how to extend a live European job-intelligence lakehouse with a frugal GenAI layer without destroying SCD Type 2 lineage or abandoning evaluation honesty. On labelled matching data, dense retrieval improved nDCG@10 over a transparent heuristic wizard on the packaging semantic-embed run. OpenAI-first cost modelling and live RQ2 runs show enrichment at cents per thousand jobs when rules-first sampling applies. Remaining work is primarily **Bedrock quota → live Nova Pareto → optional Bedrock enrichment re-enable**, not redesign.

### 9.2 Future work

1. Complete RQ2 live Pareto after Nova Micro quota approval.  
2. Expand matching labels (≥100 queries) and multilingual graded relevance.  
3. Bedrock faithfulness audit for multi-agent citations.  
4. Optional LanceDB production cutover metrics (latency, recall).  
5. Supervisor-agreed typesetting to UE binding standards; colloquium demo using `docs/DEMO_SCRIPT.md`.

### 9.3 Closing statement

DataForge demonstrates that an MSc Data Science thesis can be both a **hireable production portfolio** and a **falsifiable integration study**—provided GenAI is treated as a governed pipeline stage, not a slideware feature.

---

## References

Armbrust, M., et al. (2021). Lakehouse: A new generation of open platforms that unify data warehousing and advanced analytics. *CIDR*.

Cormack, G. V., Clarke, C. L. A., & Buettcher, S. (2009). Reciprocal rank fusion outperforms condorcet and individual rank learning methods. *SIGIR*.

European Union. (2016). Regulation (EU) 2016/679 (General Data Protection Regulation).

European Union. (2024). Regulation (EU) 2024/1689 (Artificial Intelligence Act).

Järvelin, K., & Kekäläinen, J. (2002). Cumulated gain-based evaluation of IR techniques. *ACM TOIS, 20*(4), 422–446.

Karpukhin, V., et al. (2020). Dense passage retrieval for open-domain question answering. *EMNLP*.

Kimball, R., & Ross, M. (2013). *The data warehouse toolkit* (3rd ed.). Wiley.

Yao, S., et al. (2023). ReAct: Synergizing reasoning and acting in language models. *ICLR*.

*Additional primary artefacts:* DataForge repository documentation (`docs/ARCHITECTURE.md`, `DATA_DICTIONARY.md`, `RESPONSIBLE_AI.md`, `BUDGET_RUNBOOK.md`, `evals/results/*`).

---

## Appendix A — Artefact map (GitHub)

| Artefact | Path |
|----------|------|
| Proposal (ARM-style) | `docs/thesis/THESIS_PROPOSAL.md` |
| Exposé | `docs/thesis/THESIS_EXPOSE.md` |
| Results ledger | `docs/thesis/RESULTS_DRAFT.md` |
| This paper (draft) | `docs/thesis/THESIS_PAPER.md` |
| Matching eval | `evals/results/eval_report.md` |
| ROI model | `evals/results/roi_report.json` |
| Demo / Loom script | `docs/DEMO_SCRIPT.md` |
| Match OpenAPI | `docs/openapi.match.json` |

## Appendix B — Reproduction commands

```bash
# Matching eval (RQ3)
py -3 evals/run_matching_eval.py

# Rules enrichment eval
py -3 evals/run_enrichment_rules_eval.py

# ROI model (RQ4)
py -3 scripts/roi_report.py

# RQ2 live (after Bedrock quota approval)
AWS_PROFILE=dataforge-germany AI_ENABLED=true \
  BEDROCK_COMPLETION_MODEL=eu.amazon.nova-micro-v1:0 \
  py -3 evals/run_rq2_pareto.py --live-providers --provider bedrock --limit 40
```

## Appendix C — Suggested supervisor title-page fields

- Student name / matriculation number (fill)  
- Erstbetreuung / Zweitbetreuung (fill after Betreuungsvertrag)  
- Programme: M.Sc. Data Science  
- University of Europe for Applied Sciences, Potsdam  
- Submission date (fill)  
- Declaration of independent work (UE-required wording — insert from Prüfungsamt template)

## Appendix D — Change log for this draft

| Date | Change |
|------|--------|
| 2026-09-16 | Initial full paper draft from proposal outline + measured evals; RQ2 marked pending quota |

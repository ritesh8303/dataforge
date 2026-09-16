# Integrating Multi-Provider Generative AI into a Production Medallion Lakehouse: Architecture, Retrieval Evaluation, Cost Control, and Responsible Deployment — A Case Study on European Job Intelligence (DataForge)

**Master’s Thesis (Draft)**  
**Programme:** M.Sc. Data Science (2-year)  
**Institution:** University of Europe for Applied Sciences, Potsdam  
**Author:** Ritesh Rakesh Jadhav  
**Date:** September 2026  
**Reference implementation:** https://github.com/ritesh8303/dataforge  

> **Formatting note.** Official UE / Prüfungsamt layout (margins, fonts, binding, citation style, exact page limits) must be confirmed with supervisors and the Examination Office. This document is the **content draft** aligned to the programme’s applied ARM-style structure and the chapter outline in `THESIS_PROPOSAL.md` (target ~40–80 pages when typeset, excluding appendices). Convert to Word/LaTeX before binding.

---

## Abstract

European job postings are fragmented across public employment services, aggregators, and applicant-tracking systems. DataForge is a production serverless medallion lakehouse on Amazon Web Services (AWS) that already ingests multi-source vacancy data into Bronze, Silver (SCD Type 2), and Gold layers with public Jobs/Metrics APIs. Industry demand, however, has shifted from raw aggregation toward generative-AI (GenAI) features such as semantic matching and structured enrichment—features that risk breaking lineage, exploding cost, and creating non-reproducible “AI theatre” if bolted on without measurement.

This thesis designs, implements, and evaluates a **frugal GenAI integration layer** on the existing DataForge system: a multi-provider model gateway with kill switches and budgets; rules-first enrichment with Bedrock as the primary EU-resident LLM path; hybrid BM25 + dense retrieval for Match; FastAPI Match API with PII redaction and cited explanations; optional bounded multi-agent orchestration and an MCP tool bridge. Four research questions address (RQ1) non-destructive integration under cost envelopes, (RQ2) provider trade-offs under residency and budget constraints, (RQ3) ranked relevance versus a rule-based Career Matching Wizard, and (RQ4) unit economics of the AI layer.

On a labelled matching set (42 queries × 96 jobs), dense retrieval achieves the highest nDCG@10 (0.220) versus heuristic (0.159) and BM25 (0.145), supporting the hypothesis that embedding retrieval improves ranked relevance relative to the keyword wizard—while hybrid remains the product default for lexical robustness and citation workflows. Modelled unit economics for rules-first enrichment plus embeddings on a 1,000-job scenario remain on the order of **€0.06**. Live multi-provider Bedrock completion experiments for RQ2 are **pending AWS Nova Micro account quota approval** (applied account limits were zero at measurement time); the thesis records this limitation honestly and provides a reproducible protocol to complete the Pareto table once quotas are restored.

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

`ModelRouter` implements task profiles (`enrich`, `embed`, `explain`, …) with preferred provider cascades (Bedrock → OpenAI → Anthropic → local), JSON validation, CostLogger, daily budget checks, and `AI_ENABLED` kill switch. Prompt versions live under `prompts/` with a registry. Tracing helpers support Langfuse (env-gated) and S3/CloudWatch-oriented spans.

Bedrock completion defaults to the EU inference profile **`eu.amazon.nova-micro-v1:0`** after Titan Text Express reached end-of-life on the account. Embeddings use Titan Embed Text v2 when available.

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
| Enrichment nightly Bedrock | **Paused** (Nova Micro account quota was 0; increase requested) |
| RQ2 live Bedrock Pareto | **Pending** quota approval |
| RQ3 labelled matching eval | Complete |
| RQ4 modelled ROI | Complete |

---

## 6. Evaluation results

### 6.1 Methodology overview

| Experiment | Protocol | Metric |
|------------|----------|--------|
| Matching (RQ3) | 42 graded queries × 96 jobs; BM25 / dense / hybrid / heuristic | nDCG@10, P@5, Recall@20, MRR |
| Rules enrichment | Labelled jobs for field/seniority/visa/English | Precision / Recall / F1 |
| Agent ablation | 30 queries; hybrid vs multi-agent | Citation structure validity |
| RQ2 Pareto | Rules vs pinned provider (Bedrock) | Latency, €, success rate |
| ROI model | Pricing assumptions × token scenario | € per 1k jobs / per match share |

Fixtures and runners live under `evals/`; results under `evals/results/`.

### 6.2 RQ3 — Matching quality

From `evals/results/eval_report.md`:

| Method | nDCG@10 | P@5 | Recall@20 | MRR |
|--------|--------:|----:|----------:|----:|
| bm25 | 0.1452 | 0.1619 | 0.2143 | 0.1844 |
| **dense** | **0.2203** | **0.2190** | **0.2738** | **0.2677** |
| hybrid | 0.1714 | 0.1714 | 0.2341 | 0.2344 |
| heuristic | 0.1585 | 0.1905 | 0.2401 | 0.2117 |

**Interpretation.** Dense retrieval outperforms the heuristic wizard on nDCG@10 (**H1 supported** on this gold set). Hybrid trails pure dense on this particular label distribution but remains the **product default** because Match also emphasises lexical fallback, filterability, and citation workflows that combine lexical hits with structured JD evidence. Scores are intentionally **non-saturated** (unlike trivial demos with nDCG≈1.0).

### 6.3 RQ2 — Provider trade-offs (partial)

**Available now:** rules-only Pareto point (near-zero latency, €0).  
**Blocked:** live Nova Micro completions due to applied account quotas of **0 tokens/minute and 0 tokens/day** for Nova Micro, despite higher AWS default TPM. A Service Quotas increase (cross-region Nova Micro TPM → 1,000,000) was submitted on 2026-09-16 and was **PENDING** at draft time.

This is a **measurement gap**, not an architecture gap: the runner `evals/run_rq2_pareto.py --live-providers --provider bedrock` is ready. The thesis will update Table RQ2 after approval without changing the protocol.

### 6.4 Multi-agent ablation (thesis-plus)

On the local provider, average citation **structure** validity was 1.0 for both hybrid single-shot and multi-agent (30 queries). This indicates the schema/citation contract is enforced, but **faithfulness** under Bedrock still requires a post-quota spot-check (local saturation does not prove semantic grounding).

### 6.5 Threats to validity

- Label set size (42×96) is adequate for an applied MSc pilot, not an industrial IR benchmark.  
- Dense advantage may partly reflect label construction; hybrid product choice is multi-objective.  
- ROI uses public pricing assumptions until live CostLogger traces accumulate.  
- RQ2 live cells incomplete pending quota.

---

## 7. Business / ROI analysis

### 7.1 Cost model (RQ4)

Scenario: 1,000 jobs with rules-first enrichment (≈30% LLM calls) + embeddings for corpus and 100 match requests (`scripts/roi_report.py`):

| Component | Approx. USD | Approx. EUR (FX 0.92) |
|-----------|------------:|----------------------:|
| Enrich LLM full corpus | 0.077 | 0.071 |
| Enrich rules-first (30%) | 0.023 | 0.021 |
| Embeddings | 0.044 | 0.040 |
| **Total (rules-first path)** | **0.067** | **0.062** |
| Per enriched job (rules-first) | 2.3e-5 | 2.1e-5 |

**Interpretation.** At Nova Micro / Titan Embed price points, AI enrichment + embedding for a 1k-job batch is **cents**, not tens of euros—supporting **H2/H4** directionally for this workload, provided quotas allow execution and sample rates remain controlled.

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
| RQ1 | Achieved via additive schemas, immutable SCD keys, Terraform AI module, budgets/kill switch; enrichment paused only for quota, not design failure. |
| RQ2 | Protocol ready; **live Bedrock cells pending** quota. Rules baseline established. |
| RQ3 | Dense > heuristic on nDCG@10; H1 supported on labelled set. |
| RQ4 | Modelled €/1k jobs ≪ €1 under frugal settings; live logs to follow. |

### 8.2 Responsible AI

DataForge Match is an **advisory job-discovery** aid, not an employer hiring decision system. Controls include transparency (citations, disclaimers), human oversight (HITL on low confidence), PII redaction, and sourcing documentation (`docs/RESPONSIBLE_AI.md`, `docs/DATA_SOURCING.md`). Visa heuristics are sensitive and must remain evidence-linked and non-blocking by default.

### 8.3 Limitations

1. Bedrock Nova Micro account quota blocked live RQ2 at draft time.  
2. Matching label scale is pilot-sized.  
3. Multi-agent faithfulness under real LLMs not yet spot-checked.  
4. Official UE formatting rules must still be applied in the bound version.  
5. Source coverage and `not_mentioned` visa majority limit some product KPIs.

### 8.4 Implications for practice

Applied GenAI theses should treat **quota and billing mechanics** as first-class experimental constraints—equal to model choice. “Architecture complete / measurement pending” is a valid scientific state when documented with a frozen protocol.

---

## 9. Conclusion and future work

### 9.1 Conclusion

This thesis showed how to extend a live European job-intelligence lakehouse with a frugal GenAI layer without destroying SCD Type 2 lineage or abandoning evaluation honesty. On labelled matching data, dense retrieval improved nDCG@10 over a transparent heuristic wizard. Cost modelling suggests Nova-class enrichment and embeddings are inexpensive at 1k-job scale when rules-first controls apply. Remaining work is primarily **operational measurement** (Bedrock quota → RQ2 Pareto → enrichment re-enable → faithfulness spot-check), not redesign.

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

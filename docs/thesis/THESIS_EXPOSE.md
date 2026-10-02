# Master's Thesis Exposé

**University:** University of Europe for Applied Sciences, Potsdam  
**Programme:** MSc Data Science  
**Author:** Ritesh Rakesh Jadhav  
**Date:** October 2026

---

## Working Title

**Augmented Analytics on a Production Lakehouse: Integrating Generative AI into European Job Intelligence — A Case Study on DataForge**

*(Colloquium topics: augmented analytics, hyperautomation, automation of data cleaning, digitalisation of software-intensive services.)*

---

## 1. Motivation and Problem Statement

Job data in Europe is spread across government portals (BA Jobsuche, EURES), aggregators (Arbeitnow), and many company ATS pages. DataForge already solves the collection problem. It is a serverless medallion lakehouse on AWS. It has multi-source ETL, SCD Type 2 history, Gold analytics, and public APIs.

Industry demand has moved from raw lists of jobs to **AI-augmented intelligence**. Companies face three problems:

1. **Integration:** How to add language models to existing batch pipelines without breaking data history, repeatable tests, or cost control.
2. **Model selection:** Which provider (OpenAI, Anthropic, Bedrock, local) is best on quality, cost, latency, and EU data location for a given task.
3. **Money:** Raw open-data files are easy to copy. AI features (semantic matching, enrichment) may support a higher price.

DataForge now uses rule-based keyword matching in the Career Matching Wizard, and regex for skills. These baselines are labelled as non-AI on purpose. This thesis closes the gap with controlled AI integration and empirical tests.

**Status 2 October 2026:** Match API and nightly **OpenAI** enrichment (sample rate 0.25) are live. Live RQ2 on `gpt-4o-mini` completed 40/40 jobs (~$0.0011). Amazon Bedrock Nova Micro is still quota-blocked. Modelled OpenAI-first unit cost is about €0.10 per 1,000 jobs.

---

## 2. Research Questions

| ID | Question |
|----|----------|
| **RQ1** | How can generative AI be added to an existing medallion lakehouse (Bronze→Silver→Gold→API) without breaking SCD Type 2 history, repeatable tests, or a low-budget cost limit? |
| **RQ2** | For job enrichment, semantic matching, and routing, which provider or model class wins on quality, cost, latency, and EU data location? |
| **RQ3** | Do embeddings + retrieval beat the current 60/20/20 keyword Career Matching Wizard on ranked job relevance? |
| **RQ4** | Under which pricing and product packaging does AI-on-pipeline give better unit cost than selling raw open-data files only? |

**Hypotheses:** (H1) Embedding retrieval beats keyword matching on nDCG@10. (H2) Cheap batch models are enough for enrichment, compared with frontier models. (H3) A task-aware router reduces cost at similar quality, compared with one provider. (H4) AI premium features (matching API, enrichment) have better unit economics than raw CSV licensing.

---

## 3. Existing System (DataForge)

| Layer | Technology | Role |
|-------|------------|------|
| Ingest | 4× Lambda + GitHub Actions | Fetch 5 job sources |
| Bronze | S3 Parquet | Raw daily snapshots |
| Silver | S3 Parquet (SCD Type 2) | Historical job versions |
| Gold | 12 CSVs + metrics.json | Analytics + search export |
| APIs | API Gateway → 2 Lambdas | Metrics + job search |
| UI | GitHub Pages | Dashboard, job board, wizard |

**Thesis extension:** Add an AI layer (`ai_gateway`, batch enrichment, Match API, embedding index) without rebuilding the lakehouse.

---

## 4. Proposed Architecture

```
Silver → [Batch Enrichment Lambda] → Gold (+ ai_job_enrichment.csv)
Gold → [Embedding Index] → S3
User → Match API → Gateway → Providers (OpenAI / Anthropic / Bedrock / Local)
Eval Harness → Gateway logs → Thesis metrics
```

**Controls:** JSON schema checks; the LLM never changes `job_id` or SCD keys; personal data is reduced (public vacancy text is the main input); cost budgets and stop switches; prompt versions in git.

---

## 5. Methodology

### 5.1 Implementation
- Multi-provider gateway with task types: `enrich`, `embed`, `rerank`, `summarize`
- Batch enrichment that only adds Gold artefacts
- Match API with embedding retrieval and optional LLM re-rank
- Wizard A/B toggle: rule-based versus semantic matching

### 5.2 Evaluation

| Experiment | Metric | Baseline |
|------------|--------|----------|
| Matching | nDCG@10, Precision@5, human pairwise preference (n≈40) | Rule-based wizard |
| Enrichment | Skill F1, JSON validity % | Regex SKILL_KEYWORDS |
| Model comparison | Quality–cost–latency Pareto frontier | Fixed single model |
| Router | Cost at equal quality | Always-best model |
| Operations | Failure rate, p95 latency, €/1k jobs | No AI |

### 5.3 Business analysis
A unit-cost model for freemium Match API, B2B labour reports, white-label wizard, and usage-based enrichment. Router cost logs give real €/match and €/enriched-job figures.

---

## 6. Expected Contributions

1. **Reference architecture** for adding multi-provider GenAI to serverless medallion pipelines.
2. **Empirical comparison** of commercial and EU-resident AI providers on real labour-market data.
3. **Repeatable evaluation harness** with an open non-AI matching baseline.
4. **Business framework** for AI feature pricing on open-data platforms.

---

## 7. Timeline

| Phase | Period | Deliverable |
|-------|--------|---------------|
| Literature + exposé | Sep 2026 | This document |
| Supervisor approval | Oct 2026 | Signed registration |
| AI gateway + enrichment | Sep–Nov 2026 | Working pipeline |
| Match API + evals | Oct–Dec 2026 | Experiment results |
| Thesis writing | Nov 2026–Feb 2027 | Final document |
| Submission + colloquium | Feb–Mar 2027 | Degree completion |

---

## 8. References (initial)

- Medallion architecture (Databricks); SCD Type 2 (Kimball)
- RAG and dense retrieval (Lewis et al., 2020; Karpukhin et al., 2020)
- LLMOps and model routing (Chen et al.; FrugalGPT)
- EU AI Act and GDPR for automated decision support
- DataForge project documentation (repository)

---

## 9. Supervisor Request

**First supervisor:** Prof. Dr. Iftikhar Ahmed (ML, LLMs, Responsible AI) or Prof. Dr. Rand Kouatly (software engineering, LLM applications).  
**Second supervisor:** from the colloquium second-supervisor list (for example Dr. Sami ur Rahman or Dr. Ila Chandrakar).

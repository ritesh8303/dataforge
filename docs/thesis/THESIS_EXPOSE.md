# Master's Thesis Exposé

**University:** University of Europe for Applied Sciences, Potsdam  
**Programme:** MSc Data Science  
**Author:** Ritesh Rakesh Jadhav  
**Date:** September 2026

---

## Working Title

**Integrating Multi-Provider Generative AI into an Existing Production Data Pipeline: Architecture, Model Selection, Evaluation, and Business Value — A Case Study on European Job Intelligence (DataForge)**

---

## 1. Motivation and Problem Statement

European tech job data is fragmented across government portals (BA Jobsuche, EURES), aggregators (Arbeitnow), and hundreds of company ATS pages. DataForge, developed as a capstone project, solves this with a serverless medallion lakehouse on AWS: multi-source ETL, SCD Type 2 history, Gold analytics, and public APIs.

However, industry demand has shifted from raw data aggregation to **AI-augmented intelligence**. Companies struggle with three problems:

1. **Integration:** How to add LLM capabilities to existing batch pipelines without breaking lineage, reproducibility, or cost controls.
2. **Model selection:** Which provider (OpenAI, Anthropic, Bedrock, local) offers the best quality–cost–latency trade-off for specific tasks.
3. **Monetization:** Raw open-data derivatives have weak moats; AI features (semantic matching, enrichment) may justify premium pricing.

DataForge currently uses rule-based keyword matching for its Career Matching Wizard and regex for skill extraction—honestly labeled as non-AI. This thesis closes that gap with production-grade AI integration and empirical evaluation.

---

## 2. Research Questions

| ID | Question |
|----|----------|
| **RQ1** | How can generative AI be integrated into an existing medallion lakehouse (Bronze→Silver→Gold→API) without breaking SCD Type 2 semantics, reproducibility, or free-tier cost envelopes? |
| **RQ2** | For job enrichment, semantic matching, and routing tasks, which provider/model class wins on a multi-objective score (quality, cost, latency, EU data residency)? |
| **RQ3** | Do embeddings + retrieval outperform the current 60/20/20 keyword-based Career Matching Wizard on ranked job relevance? |
| **RQ4** | Under which pricing and product packaging does AI-on-pipeline create positive unit economics versus pure open-data aggregation? |

**Hypotheses:** (H1) Embedding retrieval beats regex matching on nDCG@10; (H2) cheap batch models suffice for enrichment vs. frontier models; (H3) a task-aware router reduces cost at equal quality vs. a single provider; (H4) AI premium features (matching API, enrichment) have better unit economics than raw CSV licensing.

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

**Guardrails:** JSON schema validation; LLM never mutates `job_id` or SCD keys; PII minimization (public vacancy text only); cost budgets and kill switches; prompt version registry.

---

## 5. Methodology

### 5.1 Implementation
- Multi-provider gateway with task profiles: `enrich`, `embed`, `rerank`, `summarize`
- Batch enrichment producing additive Gold artifacts
- Match API with embedding retrieval + optional LLM rerank
- Wizard A/B toggle: heuristic vs. semantic matching

### 5.2 Evaluation

| Experiment | Metric | Baseline |
|------------|--------|----------|
| Matching | nDCG@10, Precision@5, human pairwise preference (n≈40) | Rule-based wizard |
| Enrichment | Skill F1, JSON validity % | Regex SKILL_KEYWORDS |
| Model comparison | Quality–cost–latency Pareto frontier | Fixed single model |
| Router | Cost at equal quality | Always-best model |
| Operations | Failure rate, p95 latency, €/1k jobs | No AI |

### 5.3 Business analysis
Unit-economics model for freemium Match API, B2B labour reports, white-label wizard, and usage-based enrichment—using router cost logs for real €/match and €/enriched-job figures.

---

## 6. Expected Contributions

1. **Reference architecture** for integrating multi-provider GenAI into serverless medallion pipelines.
2. **Empirical comparison** of commercial and EU-resident AI providers on real labour-market data.
3. **Reproducible evaluation harness** with open baseline (rule-based matcher).
4. **Business framework** for AI feature monetization on open-data platforms.

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

**Erstbetreuung:** Prof. Dr. Iftikhar Ahmed (ML, LLMs, Responsible AI)  
**Zweitbetreuung:** Prasanna Easwarananthan or Thiyaghamani Jeyaraman (Data Engineering + AI)

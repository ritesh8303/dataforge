# DataForge — Full Technical Guide (Easy English)

**Audience:** technical readers who want a clear explanation  
**Language level:** plain English (about IELTS band 6)  
**Region:** AWS `eu-central-1` (Frankfurt, EU)  
**Last updated:** 2026-10-02

This guide explains **every main part** of DataForge: what it is, why it exists, how data moves, which tools we use, and how AI matching works.

---

## 1. What is DataForge?

DataForge is a **job data platform** focused on **early-career data and AI jobs in the European Union**.

It collects public job posts from many sources, cleans them, stores them in a **lakehouse** on AWS, and shows them on a website. It also has an **AI Match** service that ranks jobs against a resume.

**Target roles:** fresher / junior, working student (Werkstudent), internship, thesis (Abschlussarbeit).  
**Target fields:** data engineering, data science / ML / AI, analytics, BI, and related cloud/DevOps roles.

**Hard rules (we never break these):**
- No scraping LinkedIn, Indeed, StepStone, Xing, or workingstudentjobs.de
- Keep AWS cost low (typical budget around €0–25/month, hard cap about €40)
- Be honest in claims (CV / README only claim what is really live)

---

## 2. Big picture — how the system works

```
Job sources (APIs / ATS / RSS)
        │
        ▼
   Bronze (raw data on S3)
        │
        ▼
   Silver (cleaned history, SCD Type 2)
        │
        ▼
   Gold (product CSVs + metrics)
        │
        ├── Jobs API  →  jobs.html (job board)
        ├── Metrics API → dashboard.html
        ├── Enrichment Lambda → AI labels + embeddings
        └── Match API → agent.html (resume matching)
```

Think of it like a factory:

1. **Ingest** = collect raw materials  
2. **Bronze / Silver / Gold** = clean and organise  
3. **APIs + website** = sell the product to users  
4. **AI layer** = smart ranking and labels  

---

## 3. Medallion lakehouse (Bronze → Silver → Gold)

A **lakehouse** stores large files in object storage (S3) and still supports analytics.  
**Medallion** means three quality layers with clear rules.

### 3.1 Bronze — raw layer

| Item | Meaning |
|------|---------|
| **What** | Almost raw job JSON/XML converted to Parquet |
| **Where** | S3 Bronze bucket |
| **Grain** | One file per source per day |
| **Life** | About 14 days (old files expire) |
| **Rule** | Do not invent business KPIs here |

**Parquet** is a compressed columnar file format. It is cheaper and faster to read than CSV for large tables.

### 3.2 Silver — cleaned history (SCD Type 2)

| Item | Meaning |
|------|---------|
| **What** | Deduplicated jobs with types and history |
| **Where** | S3 Silver bucket |
| **Key idea** | SCD Type 2 |

**SCD Type 2** (Slowly Changing Dimension Type 2) means: when a job changes, we do **not** overwrite the old row. We close the old version and open a new version.

Useful fields:
- `job_id` — stable id for the job
- `is_current` — is this the latest version?
- `scd_start_date` / `scd_end_date` — when this version was valid

So you can answer: “When did this job first appear?” and “When did it expire?”

### 3.3 Gold — product / analytics layer

| Item | Meaning |
|------|---------|
| **What** | CSVs ready for the website, APIs, and dbt |
| **Main file** | `all_jobs.csv` (often kept in S3; small aggregates go into git) |
| **Also** | `metrics.json`, quality report, expired jobs |

**Product gate (audience gate):** Gold board / Match prefer jobs that pass:

1. **EU** location / region  
2. **Data / AI related** field  
3. **Early career** seniority (fresher, working student, internship, thesis)

Mid/senior jobs can stay in the lakehouse for research, but they are not the main product board.

---

## 4. Data sources (where jobs come from)

All sources are **public**. We store job text, not candidate CVs.

| Source id | What it is | How we get it |
|-----------|------------|---------------|
| `arbeitnow` | Public EU tech jobs API | Daily AWS Lambda |
| `ba_api` | German Federal Employment Agency (BA Jobsuche) | Daily Lambda |
| `eures` | EU job mobility portal | GitHub Actions (twice daily) |
| `berlin_startups` | Berlin Startup Jobs RSS | Daily Lambda |
| `direct` | Company career pages via ATS feeds | Daily company-ingestor Lambda |
| `himalayas` | Remote jobs API | Optional / scheduled |
| `hn_whoishiring` | Hacker News “Who is hiring” | Optional / scheduled |

### ATS (Applicant Tracking Systems)

Many companies use tools like Greenhouse, Lever, Ashby, Workable, SmartRecruiters, Recruitee, Personio, Workday, Comeet, Pinpoint.  
Config lives in:
- `config/sources/dach_ats.json`
- `config/sources/personio_tenants.json`

These are public JSON/XML feeds — not HTML scraping of forbidden sites.

### Deduplication

The same job can appear on BA and on a company site. We merge using:
- semantic `job_id` (company + title + location style key)
- `dedup_key` (adds a short description hash)
- `source_attribution` (list of all sources that saw the job)

---

## 5. AWS compute and storage

### 5.1 Region and cost

- **Region:** `eu-central-1` (Frankfurt) — good for GDPR / EU thesis story  
- **Style:** serverless (pay when code runs)  
- **IaC:** Terraform (infrastructure as code)

### 5.2 Main Lambdas (short Python programs)

| Function | Job |
|----------|-----|
| `dataforge-ingestor` | Arbeitnow |
| `dataforge-ba-ingestor` | BA Jobsuche |
| `dataforge-company-ingestor` | ATS / direct careers |
| `dataforge-berlin-startups-ingestor` | Berlin RSS |
| `dataforge-transformer` | Bronze → Silver (SCD2) |
| `dataforge-gold-generator` | Silver → Gold + audience gate |
| `dataforge-jobs-api` | Search jobs for the website |
| `dataforge-metrics` | Dashboard KPIs |
| `dataforge-enrichment` | AI labels + embedding index |
| `dataforge-match-api` | Resume ↔ job matching |

**EventBridge** schedules most of the pipeline (evening UTC).  
**API Gateway** exposes Jobs and Metrics HTTP APIs.  
**Lambda Function URL** is used for Match (longer multi-agent calls).

### 5.3 Other AWS pieces

| Piece | Why |
|-------|-----|
| S3 Bronze / Silver / Gold | Cheap durable storage |
| IAM roles | Permissions for Lambdas |
| CloudWatch logs / metrics | Debugging and LLM cost traces |
| SSM Parameter Store | Secrets like OpenAI key (`/dataforge/openai_api_key`) |
| GitHub OIDC role | CI can assume AWS role without long-lived keys |
| Step Functions Express | Thin Silver→Gold orchestration proof (schedule often off) |
| Budgets / cost alarms | Stay under the € cap |

---

## 6. Terraform (infrastructure as code)

Folder: `terraform/`

Important files:
- `main.tf` — core lakehouse (buckets, ingest Lambdas, IAM)
- `metrics.tf` — metrics API
- `ai.tf` — enrichment + Match API + Function URL CORS
- `stepfunctions.tf` — Express workflow
- `github_oidc.tf` — GitHub → AWS login
- `budget.tf` / `cost_monitoring.tf` — money guardrails

Terraform stores state in S3 with a DynamoDB lock so two people do not apply at the same time.

**CI check:** `terraform fmt` + `terraform validate` on every push.

---

## 7. Python application code (`src/`)

### 7.1 Ingest scripts

Each source has a module, for example:
- `ingest_arbeitnow.py`
- `ingest_ba_api.py`
- `ingest_company_careers.py`
- `ingest_berlin_startups.py`
- `ingest_eures.py`
- `ingest_himalayas.py`
- `ingest_hn_whoishiring.py`

They validate rows (often with **Pydantic** models), then write Bronze Parquet.

### 7.2 Transform and Gold

- `silver_transformer.py` — SCD2 logic, dedup, typing  
- `gold_generator.py` — builds product CSVs, merges AI enrichment, runs audience gate  
- `processing/audience_gate.py` — EU × data/AI × early-career filter  

### 7.3 APIs

- `jobs_api.py` — Lambda handler for job search (filters + BM25 ranking)  
- `metrics_api.py` — KPI JSON for the dashboard  
- `match_api.py` + `api/app.py` — **FastAPI** Match service wrapped by **Mangum** for Lambda  

### 7.4 Enrichment and vectors

- `enrichment_handler.py` — scheduled enrichment Lambda entry  
- `enrichment/ai_classify.py` — LLM classification  
- `enrichment/enricher.py` — builds enrichment dataframe  
- `enrichment/schemas.py` — `JobEnrichment` contract (visa, field, seniority…)  
- `enrichment/rules_de_en.py` — rule fallback (DE/EN keywords)  
- `enrichment/esco_skills.py` — ESCO-inspired skill tags  
- `enrichment/canonical_title.py` — English canonical titles  
- `enrichment/salary_extract.py` / `language_detect.py` — cheap heuristics  
- `enrichment/classification_cache.py` — content-hash cache (local or S3)  
- `embedding_index.py` — build / load embedding index JSON  
- `vector_store.py` — LanceDB-on-S3 helper + JSON fallback  

### 7.5 Retrieval and Match helpers

- `retrieval/bm25.py` — keyword ranking (BM25)  
- `retrieval/hybrid.py` — dense + BM25 + **RRF** (Reciprocal Rank Fusion)  
- `api/match_service.py` — shared match logic  
- `api/rerank.py` — listwise LLM rerank  
- `api/seeker_profile.py` — extract skills / cities / visa from resume  
- `api/match_insights.py` — skill gaps, bilingual blurbs  
- `api/duckdb_jobs.py` — optional DuckDB search backend  
- `api/http_util.py` — CORS origins + rate limit helper  
- `security/pii.py` — redact emails/phones before LLM calls  

### 7.6 AI gateway

Folder: `src/ai_gateway/`

| Piece | Role |
|-------|------|
| `router.py` | Chooses provider, budget, kill switch, traces |
| `providers/openai_provider.py` | OpenAI chat + embeddings |
| `providers/mistral_provider.py` | EU-friendly optional fallback |
| `providers/local.py` | Offline / CI only (not silent production fallback for enrich) |
| Bedrock / Azure stubs | For DACH literacy / future quota |

**Important product rule:** enrich / rerank / explain should **not** silently fall back to fake LocalProvider labels in production. Wrong “mid” seniority was poisoning the board before.

### 7.7 Multi-agent Match

Folder: `src/agent/`

- Sequential graph (always available)  
- Optional **LangGraph** backend when installed  
- Nodes: retrieve → score → explain → critic  
- Critic checks that evidence quotes exist in the job text  
- HITL helpers + ingest review agent for ambiguous audience rows  

### 7.8 Alerts

- `alerts/saved_search.py` — match jobs to saved search rules  
- `config/saved_searches.json` — example searches  
- GitHub workflow `saved_search_alerts.yml` — daily check; needs `ALERT_WEBHOOK_URL` secret to post live  

---

## 8. AI concepts used in DataForge (simple meanings)

### 8.1 Classification

The model reads a job and returns structured labels, for example:
- `ai_field` = data_engineering  
- `ai_seniority` = working_student  
- `ai_visa_stance` = sponsorship_offered / not_mentioned / eu_citizens_only …  
- `ai_english_ok`, skills, short summary  

We use **JSON schema** (strict) so the output shape is stable.

### 8.2 Embeddings

An **embedding** is a list of numbers that represents meaning.  
Similar jobs / resumes land near each other in vector space.

We use OpenAI `text-embedding-3-small` when keys/budget allow.  
Index file: `embedding_index.json` on Gold S3 (with `content_hash` for incremental rebuilds).

### 8.3 BM25

Classic search score: rewards rare words that appear in the query and the job text.  
Good for exact skills like “Spark”, “dbt”, “Airflow”.

### 8.4 Hybrid retrieval + RRF

1. Rank with BM25  
2. Rank with dense embeddings  
3. Merge ranks with **Reciprocal Rank Fusion** (RRF)  

This is more robust than either method alone.

### 8.5 Listwise rerank

After hybrid retrieval, an LLM can reorder the top list using the full resume + dream role.  
Env flag: `MATCH_RERANK=true`.

### 8.6 Prompt registry

Prompts live under `prompts/` and `src/prompts/` (e.g. `enrich_v2.md`, `explain_v2.md`) with versions tracked by the registry.  
This makes thesis / portfolio claims about prompt control more honest.

### 8.7 TraceLogger / cost budget

Each LLM call can emit CloudWatch Embedded Metric Format (EMF) and count cost against `AI_DAILY_BUDGET_USD` (often `$5`).  
If budget is exhausted, AI calls stop (kill switch).

---

## 9. Match API (how resume matching works)

**Live URL pattern:** Lambda Function URL in `eu-central-1`  
**Auth:** header `X-API-Key` (`MATCH_API_KEY`)  
**CORS:** locked to GitHub Pages origin (+ localhost for demos)  
**Rate limit:** in-process sliding window on `/match`

### Request ideas

- `resume` — text  
- `dream_role` — e.g. “Data Engineer”  
- `location` — e.g. “Berlin”  
- `method` — `hybrid` | `bm25` | `dense` | `agent`  
- filters: `entry_level_only`, `english_ok_only`, `visa_status`, `german_level`, `data_ai_only`, `audience_only`

### Response ideas

- ranked jobs with scores  
- `citations` (reason + evidence tied to `job_id`)  
- optional skill-gap plan / bilingual blurb  
- `method` string like `hybrid_rrf+listwise`  
- cost summary when AI was used  

PII redaction runs before sending resume text to providers.

---

## 10. Frontend (GitHub Pages)

Folder: `docs/` (static HTML/JS/CSS)

| Page | Purpose |
|------|---------|
| `index.html` | Home / landing |
| `dashboard.html` | KPIs and charts |
| `jobs.html` | Job board with filters, EN/DE toggle, Save tracker |
| `agent.html` | Chat-style Match UI |
| `roi.html` | Cost / ROI notes |
| `about.html` | Project story |
| `jobs/*.html` | SEO JobPosting pages (schema.org) |
| `components/` | Shared CSS/JS (`app_tracker.js`, design system) |

**Hosting:** GitHub Actions deploys `docs/` to GitHub Pages.

**Jobs board behaviour:**
- Calls the Jobs API  
- Default filter: EU data/AI early-career product set  
- Can Save applications in **localStorage** (browser only)  

**Agent behaviour:**
- Can call Match API (hybrid or multi-agent) or fall back to client scoring via Jobs API  
- Stores Match API key locally after you paste it once  

---

## 11. Analytics engineering (dbt + DuckDB)

Folder: `dbt/`

- **dbt** = transform SQL models with tests and docs  
- **DuckDB** = fast local analytical database (no warehouse bill)

CI runs:
1. `scripts/check_quality_gate.py`  
2. `dbt run` / `dbt test` / `dbt docs generate`  

This proves industry Data Engineering skills without needing Snowflake/BigQuery spend.

---

## 12. Local developer tools

| Tool | Use |
|------|-----|
| `docker compose` | Analytics profile; optional Match API / Airflow profiles |
| Airflow DAG (local) | Documents Bronze→Silver→Gold→enrich flow (AWS still uses EventBridge) |
| `scripts/run_match_api_local.py` | Local FastAPI Match |
| `scripts/deploy_lambdas.py` | Zip `src/` and update all Lambdas + env patches |
| `scripts/generate_seo_job_pages.py` | Build JobPosting HTML + sitemap |
| `scripts/query_jobs_duckdb.py` | Query Gold with DuckDB |
| `scripts/check_new_jobs_alert.py` | Saved-search alerts |

---

## 13. CI/CD (GitHub Actions)

| Workflow | What it does |
|----------|----------------|
| `ci.yml` | Ruff lint, Pytest, Terraform fmt/validate, dbt, matching evals |
| `publish_gold.yml` | Download Gold from S3, refresh charts, SEO pages, commit small Gold aggregates |
| `deploy_dashboard.yml` | Publish Pages |
| `saved_search_alerts.yml` | Daily alerts (webhook optional) |
| `eures_scraper.yml` | EURES ingest via Actions |

**Matching evals (honest):**
- BM25 / dense / hybrid / heuristic nDCG on fixture labels  
- Enrichment rules F1  
- nDCG regression gate (do not drop more than a small threshold)  
- Faithfulness spotcheck (citations must match job text; offline fixtures in CI)

---

## 14. Security and responsible AI

### Security basics

- Match API key required in production  
- CORS not `*` anymore for Match Function URL  
- Rate limiting on Match  
- Secrets in SSM / GitHub secrets — not in git  
- PII redaction before LLM  

### Responsible AI (`docs/RESPONSIBLE_AI.md`)

- Advisory only — not legal visa advice  
- Prefer cited evidence over free text hallucinations  
- Budget + kill switch to avoid runaway spend  
- EU AI Act / GDPR awareness for a thesis portfolio  

---

## 15. End-to-end day in the life (typical schedule)

Times are approximate UTC:

1. **EURES** Actions run (morning + evening)  
2. **~20:00** EventBridge starts source Lambdas → Bronze  
3. **Transformer** builds/updates Silver SCD2  
4. **Gold generator** writes product CSVs + metrics  
5. **~21:00** Publish Gold workflow may sync aggregates + SEO to git  
6. **~21:30** Enrichment Lambda samples jobs, writes AI CSV + embeddings  
7. Website and Match read the new Gold / index  

---

## 16. Important config and environment variables

| Variable | Meaning |
|----------|---------|
| `GOLD_BUCKET` | S3 Gold bucket name |
| `AI_ENABLED` | Turn AI calls on/off |
| `AI_DAILY_BUDGET_USD` | Daily LLM spend cap |
| `OPENAI_API_KEY` / `OPENAI_API_KEY_SSM` | OpenAI access |
| `MATCH_API_KEY` | Protect Match endpoint |
| `ALLOWED_ORIGIN` | CORS allow-list |
| `INDEX_BUILD_LIMIT` | Max jobs to embed (e.g. 2500) |
| `AI_ENRICHMENT_SAMPLE_RATE` | Fraction of jobs for LLM enrich |
| `ENRICHMENT_MAX_LLM` | Cap LLM enrich calls per run |
| `MATCH_RERANK` | Enable listwise rerank |
| `MATCH_BUILD_INDEX_ON_MISS` | Usually `false` in prod (avoid surprise cost) |
| `CLASSIFICATION_CACHE_S3` | Shared classification cache |
| `VECTOR_STORE_URI` | Optional LanceDB S3 path |
| `ALERT_WEBHOOK_URL` | Slack/Discord for saved searches |

---

## 17. Tests and quality

- **Pytest** unit/integration tests under `tests/`  
- **Ruff** lint (vendored Lambda packages excluded in `pyproject.toml`)  
- **Quality gate** on Gold aggregates  
- **dbt tests** for nulls / uniqueness / valid sources  
- **Eval suite** under `evals/` for matching quality  

---

## 18. What is live vs what is optional

**Usually live on AWS today:**
- Medallion ETL, Jobs/Metrics APIs, Pages UI  
- Enrichment Lambda + Match Function URL (OpenAI-first)  
- Hybrid match + listwise rerank flags  

**In repo / optional:**
- LangGraph dependency inside Lambda zip (may need packaging)  
- Mistral key (if unset, cascade skips it)  
- Live alert webhook (dry-run without secret)  
- Bedrock as primary LLM (quota may still be blocked)  

Always check README honesty notes before writing this on a CV.

---

## 19. Glossary (quick dictionary)

| Term | Easy meaning |
|------|--------------|
| **ETL** | Extract, Transform, Load — move and clean data |
| **Lakehouse** | Data lake + table habits for analytics |
| **Medallion** | Bronze / Silver / Gold quality layers |
| **SCD2** | Keep history of changes, not only latest row |
| **Parquet** | Efficient columnar file format |
| **Lambda** | Run code without managing a server |
| **API Gateway** | HTTP front door to Lambdas |
| **Function URL** | Direct HTTPS URL for one Lambda |
| **Terraform** | Code that creates cloud resources |
| **OIDC** | Safe short-lived cloud login from GitHub |
| **FastAPI** | Modern Python web API framework |
| **Mangum** | Adapter so FastAPI runs on Lambda |
| **BM25** | Keyword search ranking |
| **Embedding** | Meaning vector for text |
| **RRF** | Merge two ranked lists fairly |
| **Rerank** | LLM reorders the top results |
| **dbt** | SQL transforms + tests for analytics |
| **DuckDB** | Local analytical SQL engine |
| **ESCO** | EU skills/occupations vocabulary (we use an inspired subset) |
| **HITL** | Human-in-the-loop review |
| **PII** | Personal data (email, phone…) |
| **CORS** | Browser rule for which websites may call an API |
| **nDCG** | Ranking quality metric used in evals |

---

## 20. How to explain DataForge in one interview answer

> “DataForge is an EU early-career data/AI job lakehouse on AWS. We ingest public APIs and ATS feeds into a Bronze–Silver–Gold pipeline with SCD Type 2 history, publish a Jobs/Metrics API and GitHub Pages board, then add a frugal GenAI layer: structured enrichment, hybrid BM25 + embedding retrieval with RRF, optional listwise rerank, and a FastAPI Match service with citations, visa-aware filters, budgets, and CI eval gates.”

---

## 21. Where to read more in this repo

| Doc | Content |
|-----|---------|
| `README.md` | Short live status |
| `docs/ARCHITECTURE.md` | Layer contracts |
| `docs/DATA_SOURCING.md` | Sources and hard nos |
| `docs/DATA_DICTIONARY.md` | Field meanings |
| `docs/RESPONSIBLE_AI.md` | AI governance |
| `docs/BUDGET_RUNBOOK.md` | Cost operations |
| `docs/ROADMAP.md` | What we build next |
| `docs/DEMO_SCRIPT.md` | Demo flow |
| `docs/PROJECT_DOCUMENTATION.md` | Older long formal doc |

---

*This guide is for learning and interviews. If something in production differs, trust the live AWS config and the latest README status over any single document.*

# DataForge — Complete Final Project Documentation

**Version:** 1.0  
**Repository:** `dataforge`  
**Domain:** European / German technology job market data lakehouse  
**Deployment region:** AWS `eu-central-1` (Frankfurt)  
**Last documented:** June 2026  

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [System Architecture](#2-system-architecture)
3. [Folder and File Structure](#3-folder-and-file-structure)
4. [Core Business Logic](#4-core-business-logic)
5. [KPIs, Metrics, and Calculations](#5-kpis-metrics-and-calculations)
6. [Data Storage Design (Lakehouse Schema)](#6-data-storage-design-lakehouse-schema)
7. [API Documentation](#7-api-documentation)
8. [Frontend Logic](#8-frontend-logic)
9. [Authentication and Security](#9-authentication-and-security)
10. [Technology Stack Justification](#10-technology-stack-justification)
11. [End-to-End System Flow](#11-end-to-end-system-flow)
12. [Edge Cases and Error Handling](#12-edge-cases-and-error-handling)
13. [Future Improvements](#13-future-improvements)

---

## 1. Project Overview

### 1.1 Non-Technical Explanation

**DataForge** is a **Job Intelligence Platform** that aggregates, processes, and analyzes 10,000+ jobs across Europe using a multi-source ETL pipeline. It collects job postings from multiple European sources (German federal job board, EU mobility portal, startup boards, company career pages, and more), cleans and deduplicates them, and presents them through:

- A **Job Intelligence Dashboard** (trends, skills, locations, sources)
- A **Job Intelligence Board** with filters (source, country, remote, language)
- A **Career Matching Wizard** — rule-based job intelligence matching (client-side resume matching against live jobs)

The system runs **without traditional servers**. It uses AWS Lambda (short-lived functions), S3 storage, and a static website on GitHub Pages. Costs are designed to stay within AWS Free Tier where possible.

### 1.2 Technical Explanation

DataForge implements a **medallion architecture** data lakehouse:

| Layer | Format | Purpose |
|-------|--------|---------|
| **Bronze** | Parquet (partitioned by `ingested_at`) | Raw normalized ingest per source |
| **Silver** | Parquet (SCD Type 2, `is_current` partitions) | Historical job records with change tracking |
| **Gold** | CSV aggregates | Analytics-ready datasets + job search export |

Ingestion is **scheduled** (EventBridge cron) and **event-driven** (Silver write triggers Gold generator). A separate **EURES** ingestor runs on GitHub Actions due to WAF/timeout constraints on Lambda.

### 1.3 Problem Statement

European tech job seekers and analysts face:

- **Fragmentation** — jobs spread across BA Jobsuche, EU portals, niche boards, and company sites
- **Stale duplicates** — same role posted on multiple platforms
- **No unified analytics** — no single view of market trends, skills demand, or regional distribution
- **High infrastructure cost** — traditional warehouses (Glue, Redshift, Athena) exceed student/hobby budgets

### 1.4 Industry Context

The project targets the **labour market intelligence** domain, specifically:

- **Germany** — strong public employment APIs (Bundesagentur für Arbeit)
- **EU mobility** — EURES cross-border vacancies
- **Tech hiring** — startup boards and ATS APIs (Greenhouse, Lever, SmartRecruiters, and more)

Competing commercial aggregators exist (LinkedIn, Indeed, Glassdoor) but are closed, expensive, or geographically narrow. DataForge is an **open, serverless, reproducible pipeline** optimized for European data roles.

### 1.5 Why This Solution Is Needed

| Requirement | DataForge approach |
|-------------|-------------------|
| Low cost | Lambda + S3 + GitHub Pages (~$0/month at dev scale) |
| Historical accuracy | SCD Type 2 tracks job changes and expirations |
| Multi-source | 7 ingest paths with unified schema |
| Public access | HTTP APIs + static dashboard without login friction |
| Extensibility | New ingestor = new Lambda handler + Bronze prefix |

---

## 2. System Architecture

### 2.1 High-Level Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES (External)                          │
│  Arbeitnow API │ BA Jobsuche │ EURES API │ Berlin RSS │ Direct ATS APIs      │
│  (5 active sources — company career pages across 10 ATS families)           │
└───────────────┬─────────────────────────────────────────────────────────────┘
                │
    ┌───────────┼───────────┬──────────────────────┐
    │           │           │                      │
    ▼           ▼           ▼                      ▼
┌────────┐ ┌────────┐ ┌──────────────┐    ┌─────────────────┐
│ Lambda │ │ Lambda │ │ GitHub Action│    │ Lambda (×6)     │
│Ingestors│ │Ingestors│ │ EURES scraper│    │ scheduled cron  │
└────┬───┘ └────┬───┘ └──────┬───────┘    └────────┬────────┘
     │          │            │                     │
     └──────────┴────────────┴─────────────────────┘
                              │
                              ▼
              ┌───────────────────────────────┐
              │  S3 BRONZE (Parquet)            │
              │  {source}/ingested_at=DATE/     │
              └───────────────┬───────────────┘
                              │ EventBridge cron
                              ▼
              ┌───────────────────────────────┐
              │  Lambda: dataforge-transformer  │
              │  SCD Type 2 → Silver            │
              └───────────────┬───────────────┘
                              │ S3 ObjectCreated (.parquet)
                              ▼
              ┌───────────────────────────────┐
              │  Lambda: dataforge-gold-generator│
              │  Aggregations → Gold CSVs       │
              └───────────────┬───────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
   ┌────────────────────┐         ┌─────────────────────┐
   │ S3 GOLD (*.csv)    │         │ GitHub dispatch     │
   └─────────┬──────────┘         │ → publish_gold.yml  │
             │                    └─────────────────────┘
             │
     ┌───────┴────────┐
     ▼                ▼
┌─────────────┐  ┌─────────────┐
│ Lambda:     │  │ Lambda:     │
│ metrics-api │  │ jobs-api    │
└──────┬──────┘  └──────┬──────┘
       │                │
       ▼                ▼
┌─────────────────────────────────────┐
│ API Gateway HTTP API (eu-central-1) │
└──────────────────┬──────────────────┘
                   │ HTTPS GET (CORS)
                   ▼
        ┌──────────────────────────┐
        │ GitHub Pages (docs/)     │
        │ index.html │ dashboard.html │
        │ jobs.html  │ agent.html     │
        └──────────────────────────┘
```

### 2.2 Frontend / Backend / Storage / Services

| Layer | Technology | Responsibility |
|-------|------------|----------------|
| **Frontend** | Static HTML/CSS/JS (`docs/`) | Landing page, Job Intelligence Dashboard, Job Intelligence Board, Career Matching Wizard |
| **API layer** | AWS API Gateway HTTP API | Routes `GET /` to Lambda proxies |
| **Compute** | AWS Lambda (Python 3.11) | Ingest, transform, aggregate, serve APIs |
| **Storage** | AWS S3 (3 buckets) | Bronze, Silver, Gold data |
| **Orchestration** | EventBridge + S3 notifications + GitHub Actions | Schedules and triggers |
| **IaC** | Terraform | Reproducible infrastructure |
| **CI/CD** | GitHub Actions | Lint, test, EURES scrape, Gold publish, Pages deploy |
| **Secrets** | SSM Parameter Store | API keys (String tier, not SecureString) |

There is **no traditional application server**, **no relational database**, and **no container cluster**.

### 2.3 Data Flow (Step-by-Step)

#### Phase A — Ingestion (Bronze)

1. **EventBridge** fires at scheduled UTC times (default 07:00, 12:00, 16:00 for most ingestors).
2. Each **ingestor Lambda** calls its external API/RSS/ATS endpoint.
3. Raw items are **normalized** to a unified column schema (`job_id`, `title`, `company`, `location`, `url`, `description`, `remote`, `tags`, `job_types`, `source`, `ingested_at`).
4. A **pandas DataFrame** is written via `processing.utils.save_parquet()` to:
   - `s3://dataforge-bronze-dev-eu-central-1/{source}/ingested_at={YYYY-MM-DD}/jobs.parquet`
5. **EURES** follows the same Bronze path but runs from **GitHub Actions** at 04:00 UTC, then asynchronously invokes the Silver transformer.

#### Phase B — Silver (SCD Type 2)

1. **Transformer Lambda** (`dataforge-transformer`) runs on cron (07:30, 12:30, 16:30 UTC).
2. Lists all Bronze Parquet files for today's `ingested_at` partition across sources.
3. Concatenates, **validates** (non-empty title/company/url), applies **Europe safety gate**, **deduplicates** semantically.
4. Loads existing Silver Parquet (`is_current=True` and `is_current=False`).
5. Compares **hash keys** on `(title, company, location, source, job_types, url)` per `job_id`.
6. **New jobs** → insert as current. **Changed jobs** → expire old row, insert new version. **Unchanged** → retain.
7. Writes updated partitions to Silver bucket.
8. Writes `pipeline_stats.csv` to Gold with run metrics.

#### Phase C — Gold

1. **S3 notification** on Silver Parquet `ObjectCreated` invokes `dataforge-gold-generator`.
2. Gold generator reads full Silver history, enriches active jobs (tags, language, work style, region).
3. Produces **11 CSV files** + writes to Gold bucket.
4. Optionally fires **GitHub `repository_dispatch`** (`gold_data_updated`) for Pages/data refresh.

#### Phase D — Consumption

1. **Metrics API** reads Gold CSVs, builds JSON payload, caches 2 minutes.
2. **Jobs API** reads `all_jobs.csv` or `expired_jobs.csv`, filters/sorts/paginates, caches 5 minutes.
3. **GitHub Pages** serves `docs/*.html` which `fetch()` the APIs from the browser.
4. **publish_gold.yml** downloads Gold to `data/gold/` in the repo for offline analysis.

### 2.4 Component Communication

| From | To | Protocol | Payload |
|------|-----|----------|---------|
| Browser | API Gateway | HTTPS GET | Query parameters |
| API Gateway | Lambda | AWS_PROXY | API Gateway event JSON |
| Lambda | S3 | boto3 / awswrangler | Parquet, CSV |
| EventBridge | Lambda | Async invoke | `{}` or `{"date": "YYYY-MM-DD"}` |
| S3 Silver | Gold Lambda | Lambda invoke permission | S3 event notification |
| Gold Lambda | GitHub API | HTTPS POST | `repository_dispatch` |
| GitHub Actions | EURES API | HTTPS POST | JSON search body |
| GitHub Actions | AWS Lambda | boto3 invoke | Transformer trigger |

### 2.5 Authentication Flow

**End users:** No login. Dashboard and job board are **public read-only** APIs with CORS `*` (configurable via `ALLOWED_ORIGIN` for metrics).

**AWS services:** Lambda execution roles use **IAM policies** (S3 read/write on project buckets, SSM read, CloudWatch logs). API Gateway invokes Lambda via **resource-based policies**.

**External APIs:**
- BA Jobsuche: public `X-API-Key: jobboerse-jobsuche`
- GitHub redeploy: token from env or SSM `/dataforge/dev/github_token`

### 2.6 Request–Response Lifecycle (Jobs API Example)

1. User selects "EURES" on `jobs.html` → JavaScript calls `loadJobs()`.
2. `fetchAllJobPages()` loops with `limit=2000` and increasing `offset`.
3. Browser sends: `GET https://{jobs-api}/?source=eures&limit=2000&offset=0&sort=newest&status=active&board=2026-06-08.5&_=timestamp`
4. API Gateway routes to `dataforge-jobs-api` Lambda.
5. Lambda checks in-memory cache (keyed by `status=active`); if expired, loads `all_jobs.csv` from S3.
6. Filters: `source == "eures"`.
7. Sorts by `date_added` descending.
8. Slices `[offset : offset+limit]`.
9. Returns JSON: `{ jobs: [...], kpis: { total, filtered, returned, truncated, ... }, cached_at }`.
10. Frontend populates cards, country dropdown, pagination (25 per page).

---

## 3. Folder and File Structure

The repository contains **79 tracked project files** (excluding `.venv` and bundled Lambda vendor packages under `src/`).

```
dataforge/
├── .github/workflows/     # CI/CD automation
├── data/gold/             # Committed Gold CSV snapshots + dashboard PNG
├── docs/                  # GitHub Pages static frontend + this document
├── scripts/               # Local development and operations tooling
├── src/                   # Lambda handlers and shared processing library
├── terraform/             # AWS infrastructure as code
├── tests/                 # Unit and integration tests
├── README.md              # Quick-start and architecture summary
├── pyproject.toml         # Ruff linter configuration
├── pytest.ini             # Test runner configuration
├── requirements-test.txt  # Dev/test Python dependencies
└── .gitignore             # Ignore rules (venv, terraform state, local bronze)
```

### 3.1 Root Configuration Files

#### `README.md`
- **Purpose:** Project introduction, architecture diagram, data sources, local run commands, Terraform instructions.
- **Connections:** References `scripts/`, `terraform/`, Gold CSV list.

#### `pyproject.toml`
- **Purpose:** Ruff lint configuration (line length 120, excludes bundled `src/` vendor dirs).
- **Connections:** Used by `.github/workflows/ci.yml` lint step.

#### `pytest.ini`
- **Purpose:** Sets `testpaths = tests`, `pythonpath = src`.
- **Connections:** All `tests/test_*.py` modules.

#### `requirements-test.txt`
- **Purpose:** Dev dependencies: `pytest`, `pytest-mock`, `moto`, `pandas`, `boto3`, `awswrangler`, `pydantic`.
- **Connections:** CI test job, local testing.

#### `.gitignore`
- **Purpose:** Excludes secrets (`.env`, `*.tfvars`), Terraform state, venv, Lambda zips, bundled `src/` packages, `data/bronze/`, `data/silver/`, temp debug files.
- **Note:** `data/gold/` is **intentionally committed** for offline analysis and CI publish.

---

### 3.2 `.github/workflows/`

#### `ci.yml` — Continuous Integration
| Step | Action |
|------|--------|
| Trigger | Push/PR to `main` |
| Lint | `ruff check src/ scripts/ tests/` |
| Test | `pytest tests/test_scd_logic.py tests/test_architecture_smoke.py tests/test_eures_ingestor.py` |

#### `eures_scraper.yml` — EURES Daily Ingest
| Step | Action |
|------|--------|
| Trigger | Cron `0 4 * * *`, manual dispatch |
| Run | `ingest_eures.lambda_handler` with AWS credentials |
| Chain | Async invoke `dataforge-transformer` with today's UTC date |

#### `publish_gold.yml` — Gold Snapshot to Repository
| Step | Action |
|------|--------|
| Trigger | Push `main`, cron `30 8 * * *`, `repository_dispatch: gold_data_updated` |
| Download | `python scripts/download_all.py` |
| Visualize | `python scripts/visualize_gold.py` |
| Commit | `data/gold/*.csv`, `job_market_dashboard.png` |

#### `deploy_dashboard.yml` — GitHub Pages
| Step | Action |
|------|--------|
| Trigger | Push to `main` when `docs/**` changes |
| Deploy | Upload `docs/` artifact to GitHub Pages |

---

### 3.3 `src/` — Lambda Application Code

#### `src/__init__.py`
Package marker for `src` module.

#### `src/requirements.txt`
Runtime pip deps for Lambda deployment: `pydantic`, `requests`, `typing_extensions`. (`pandas`, `awswrangler`, `boto3` provided via Lambda layer.)

---

#### `src/ingest_arbeitnow.py`
| Item | Detail |
|------|--------|
| **Purpose** | Bronze ingestor for Arbeitnow public API |
| **Entry** | `lambda_handler(event, context)` |
| **Fetcher** | `ArbeitnowFetcher` — max **2 pages** |
| **Normalization** | `company_name→company`, `slug→job_id`, list cols → CSV strings |
| **Output** | `arbeitnow/ingested_at={date}/jobs.parquet` |
| **Env** | `BRONZE_BUCKET`, `LOCAL_RUN` |

#### `src/ingest_ba_api.py`
| Item | Detail |
|------|--------|
| **Purpose** | Bronze ingestor for Bundesagentur für Arbeit Jobsuche v4 |
| **Queries** | 23 search terms (data/tech roles, Werkstudent, Praktikum, etc.) |
| **Pagination** | 100 jobs/page until `maxErgebnisse` |
| **Dedup** | By `refnr` across queries |
| **Normalization** | German field names → English; flatten `arbeitsort`; build URL from refnr |
| **Output** | `ba_api/ingested_at={date}/jobs.parquet` |
| **Source key** | `ba_api` |

#### `src/ingest_eures.py`
| Item | Detail |
|------|--------|
| **Purpose** | EURES EU portal ingestor (GitHub Actions, not Terraform Lambda) |
| **API** | `POST https://europa.eu/eures/api/jv-searchengine/public/jv-search/search` |
| **Keywords** | 10 terms searched **individually** (API ANDs combined keywords) |
| **Limits** | 50 results/page, default 5 pages/keyword (`EURES_MAX_PAGES_PER_KEYWORD`) |
| **Class** | `AntigravityClient` — HTTP retry shim |
| **Functions** | `build_search_payload`, `extract_location`, `extract_tags`, `normalize_eures_job`, `_job_url`, `fetch_eures_jobs` |
| **URL format** | `/jv-details/{encoded_id}?jvDisplayLanguage={lang}` |
| **Output** | `eures/ingested_at={date}/jobs.parquet` |

#### `src/ingest_berlin_startups.py`
| Item | Detail |
|------|--------|
| **Purpose** | Berlin Startup Jobs RSS ingestor |
| **Feeds** | `berlinstartupjobs.com/feed/`, `/engineering/feed/` |
| **Filter** | Tech keyword filter in fetcher |
| **Output** | `berlin_startups/ingested_at={date}/jobs.parquet` |

#### `src/ingest_company_careers.py`
| Item | Detail |
|------|--------|
| **Purpose** | Multi-ATS direct company career page scraper |
| **ATS supported** | Greenhouse, Lever, Ashby, Workable, SmartRecruiters, Recruitee, Personio XML, Workday CXS, Comeet, Pinpoint |
| **Config** | `DEFAULT_TARGETS` (~40 companies) + optional S3/URL/inline JSON registry |
| **Concurrency** | `ThreadPoolExecutor`, default 24 workers |
| **Cap** | `MAX_JOBS_PER_COMPANY=2000` |
| **Filter** | `_is_europe_job()` per posting |
| **Output** | `direct_careers/ingested_at={date}/jobs.parquet`, `source=direct` |

#### `src/silver_transformer.py`
| Function | Purpose |
|----------|---------|
| `slugify(text)` | Normalize text for semantic matching keys |
| `validate_jobs(df)` | Drop invalid rows (missing title/company/url) |
| `deduplicate_bronze(df)` | Cross-source dedup within daily batch |
| `generate_hash(df, cols)` | SHA-256 change detection |
| `process_scd_type_2(...)` | Full SCD Type 2 merge logic |
| `lambda_handler(...)` | Orchestrate Bronze read → transform → Silver write |

**Source priority for dedup (lower = wins):** `direct` < `eures` < `arbeitnow` < `berlin_startups` < `ba_api`

**Active sources (5):** `ba_api`, `direct`, `eures`, `arbeitnow`, `berlin_startups`

---

#### `src/gold_generator.py`
| Function | Purpose |
|----------|---------|
| `detect_is_english(row)` | EN vs DE word frequency in title+description |
| `detect_language_requirement(row)` | `english_only` vs `german_required` |
| `detect_work_style(row)` | `remote` / `hybrid` / `onsite` |
| `lambda_handler(...)` | Load Silver, enrich, write 11 Gold CSVs |
| `_trigger_github_redeploy(...)` | POST GitHub `repository_dispatch` |

---

#### `src/jobs_api.py`
| Function | Purpose |
|----------|---------|
| `_is_options(event)` | CORS preflight detection |
| `lambda_handler(event, context)` | Filter, sort, paginate jobs from Gold CSV |

**Cache:** 300 seconds per `status` key (`active` / `expired`).

---

#### `src/metrics_api.py`
| Function | Purpose |
|----------|---------|
| `_read_csv(bucket, key)` | S3 CSV → list of dicts |
| `_build_payload(bucket)` | Aggregate all Gold CSVs into dashboard JSON |
| `lambda_handler(event, context)` | Cached metrics response (120s TTL) |

---

#### `src/processing/utils.py`
| Function | Purpose |
|----------|---------|
| `save_parquet(df, path, source)` | Write Parquet to S3 or `data/bronze/` if `LOCAL_RUN=true` |

#### `src/processing/fetchers.py`
| Class | API | Limits |
|-------|-----|--------|
| `ArbeitnowFetcher` | `arbeitnow.com/api/job-board-api` | 2 pages |
| `BAFetcher` | `rest.arbeitsagentur.de/.../v4/jobs` | Full pagination, 100/page |
| `BerlinStartupJobsFetcher` | 2 RSS feeds | All items, tech filter |
| `get_fetcher(source)` | Factory | — |

#### `src/processing/europe_filter.py`
| Function | Purpose |
|----------|---------|
| `is_in_europe(...)` | Blocklist non-EU; allowlist EU countries/cities |
| `classify_region(...)` | Map location → country name, `Remote`, or `Other` |

#### `src/processing/typing_inspection/arbeitnow.py`
- Pydantic `TypeAdapter` validation for Arbeitnow API JSON schema.
- `validate_api_response(raw_json)` used by `ArbeitnowFetcher`.

#### `src/processing/typing_inspection/ba_api.py`
- TypedDict definitions for BA API response.
- `validate_ba_response()` — defined but **not currently called** by `BAFetcher`.

---

### 3.4 `terraform/`

#### `terraform/main.tf`
- S3 modules: bronze, silver, gold buckets
- IAM module: Lambda role with S3 + SSM + CloudWatch permissions
- SSM: `ba_api_credentials` = `jobboerse-jobsuche`
- Lambda modules: 6 ingestors + transformer + gold generator
- S3 notification: Silver Parquet → gold generator Lambda

#### `terraform/metrics.tf`
- Lambdas: `dataforge-metrics`, `dataforge-jobs-api`
- API Gateway HTTP APIs with CORS, throttling (burst 5, rate 3)
- Outputs: API invoke URLs for frontend

#### `terraform/modules/s3/`
Creates versioned-disabled bucket, 30-day lifecycle, AES256 encryption, blocked public access.

#### `terraform/modules/lambda/`
Zip from `../src`, DLQ, CloudWatch logs (7-day), optional SNS error alarms, optional EventBridge schedule.

#### `terraform/modules/iam/`
Role `dataforge-lambda-role-dev` with bucket-scoped S3 policies and SSM read on `/dataforge*`.

#### `terraform/variables.tf` + `terraform.tfvars.example`
`alert_email`, `dashboard_origin`, company careers config URI/URL/mode.

---

### 3.5 `docs/` — Frontend

#### `docs/index.html` — Landing Page (Job Intelligence Platform)
- Product homepage with live stats, feature overview, and data pipeline visualization
- Fetches `METRICS_API_URL` for hero and stats section KPIs

#### `docs/dashboard.html` — Job Intelligence Dashboard
- Fetches `METRICS_API_URL` every 2 minutes
- Renders KPI cards, Chart.js trend line, doughnut charts, bar charts
- Uses: `total_jobs`, `new_today`, `trend`, `jobs_by_source`, `jobs_by_region`, `top_skills`, `remote_vs_onsite`, `description_insights`, `top_companies`

#### `docs/jobs.html` — Job Intelligence Board
- Fetches `JOBS_API_URL` with paginated loading (2000 jobs/page)
- Filters: source, work style, language, region, city, search, experience
- Renders 25 jobs/page with Apply links
- Build version cache-bust: `BOARD_BUILD = "2026-06-08.5"`

#### `docs/agent.html` — Career Matching Wizard (client-side)
- 4-step wizard: resume → dream role → location → results
- Fetches up to 800 jobs matching dream role search
- `extractSkills()`, `detectSeniority()`, weighted match score (60% skills, 20% seniority, 20% location)
- Rule-based job intelligence matching — heuristic scoring only (no backend LLM)

#### `docs/.gitkeep`
Placeholder to retain empty docs directory in git.

---

### 3.6 `scripts/` — Local Tooling

| File | Purpose |
|------|---------|
| `paths.py` | `ROOT`, `GOLD_DIR`, `BRONZE_DIR`, `SRC_DIR` path constants |
| `download_all.py` | Download 12 Gold CSVs from S3 → `data/gold/` |
| `query_gold.py` | Regenerate subset of Gold from Silver (local + S3 write) |
| `query_duckdb.py` | Ad-hoc DuckDB SQL on local `data/gold/*.csv` |
| `visualize_gold.py` | Matplotlib dashboard PNG → `data/gold/job_market_dashboard.png` |
| `run_local_api.py` | HTTP server `:8000` mocking S3, serving docs + Lambda handlers |
| `run_ingestor_local.py` | CLI: `python scripts/run_ingestor_local.py {ba\|arbeitnow\|berlin\|direct\|eures}` |
| `backfill_silver.py` | One-time Bronze JSON → Silver SCD backfill |

---

### 3.7 `data/gold/` — Committed Gold Snapshots

| File | Description |
|------|-------------|
| `all_jobs.csv` | Active jobs for Jobs API (primary consumer) |
| `expired_jobs.csv` | Historically expired jobs |
| `jobs_by_source.csv` | Count per `source` |
| `jobs_by_region.csv` | Count per `region` |
| `top_locations.csv` | Top 20 cities |
| `top_companies.csv` | Top 20 employers |
| `remote_vs_onsite.csv` | Remote vs on-site (arbeitnow + direct) |
| `jobs_trend.csv` | New jobs per day (first appearance) |
| `active_vs_expired.csv` | Active vs expired totals |
| `top_skills.csv` | Top 20 extracted skills |
| `description_insights.csv` | Arbeitnow description analytics summary row |
| `pipeline_stats.csv` | Last Silver SCD run stats |
| `job_market_dashboard.png` | Generated matplotlib chart |

---

### 3.8 `tests/`

| File | Tests |
|------|-------|
| `conftest.py` | Strips bundled `src/` from path; `mock_aws` S3 fixtures |
| `test_scd_logic.py` | SCD Type 2: new, unchanged, changed, expired, errors |
| `test_architecture_smoke.py` | Pydantic TypedDict validation smoke |
| `test_eures_ingestor.py` | EURES location, tags, URL, retry, handler |
| `test_company_careers.py` | ATS URL detection, fetch mocks, Europe filter |
| `test_ba_model.py` | BA Pydantic validation |
| `test_europe_filter.py` | `is_in_europe`, `classify_region` parametrized |
| `test_jobs_api_local.py` | Jobs API filters with mocked S3 |
| `test_e2e_pipeline.py` | **Live AWS** integration (not in CI) |

---

## 4. Core Business Logic

### 4.1 Bronze Normalization Algorithm

**Input:** Heterogeneous API/RSS/ATS records.  
**Output:** Unified flat schema per row.

**Steps (per ingestor):**
1. Extract fields with source-specific key fallbacks (`title` / `positionName` / `jobTitle`).
2. Assign stable `job_id` with source prefix (`eures_`, `direct_{ats}_`, `bsj_`, `ba_`, etc.).
3. Coerce `remote` to boolean via flags or keyword heuristics.
4. Flatten list fields (`tags`, `job_types`) to comma-separated strings for Parquet.
5. Set `source` identifier and `ingested_at` ISO timestamp.
6. Write single daily Parquet partition per source.

### 4.2 Europe Safety Gate (Silver)

```python
def row_is_in_europe(r):
    if source in ("ba_api", "arbeitnow", "berlin_startups", "eures"):
        return True  # trusted European sources
    return is_in_europe(location, title, description)
```

**Rationale:** BA, Arbeitnow, Berlin, and EURES are inherently EU-focused. Direct ATS feeds require geo validation to avoid US-only noise.

### 4.3 Semantic Deduplication (Bronze Batch)

**Key generation:**
```
semantic_key = "sem_" + slugify(company) + "_" + slugify(title) + "_" + slugify(location)
```

**Priority sort:** Lower priority number wins. Ties broken by longer `description`.

**Effect:** Same job appearing from BA and EURES in one daily batch keeps the higher-quality source record.

### 4.4 SCD Type 2 Algorithm (Detailed)

**Change hash columns:** `title`, `company`, `location`, `source`, `job_types`, `url`

**For each incoming Bronze batch:**

| Step | Operation |
|------|-----------|
| 1 | Compute `hash_key = SHA256(concat(attr_cols))` for each Bronze row |
| 2 | Load all Silver rows (active + historical) |
| 3 | If Silver empty → **initial load** (overwrite active partition) |
| 4 | **Quality gate:** abort if active Silver > 1000 and Bronze < 100 |
| 5 | Outer merge Bronze vs active Silver on `job_id` |
| 6 | **Changed:** `hash_old ≠ hash_new` → expire active row (`is_current=False`, `scd_end_date=now`) |
| 7 | **New:** `job_id` not in Silver → insert |
| 8 | **Insert set:** new IDs ∪ changed IDs (new version for changed) |
| 9 | **Unchanged:** active rows not in changed set → keep as-is |
| 10 | Overwrite `is_current=True/` partition; append to `is_current=False/` |
| 11 | Write `pipeline_stats.csv` to Gold |

**Temporal columns:**
- `scd_start_date` — when this version became current
- `scd_end_date` — when superseded (NaT if still current)
- `is_current` — boolean active flag

### 4.5 Gold Enrichment Logic

Applied to **active** Silver rows before CSV export:

#### Tag enrichment (`enrich_tags`)
Regex patterns add system tags: `AI / ML`, `Data Engineering`, `Cloud / DevOps`, `Analytics / BI`, `Forward Deployed`, `Junior / Entry Level`, `Working Student`, `Internship`, `Master Thesis`.

#### Language detection
- `detect_is_english`: count EN function words vs DE; fallback to English job-title keywords.
- `detect_language_requirement`: if German skill patterns found → `german_required`, else `english_only`.

#### Work style
- `hybrid` if hybrid keywords in text
- else `remote` if `remote=True`
- else `onsite`

#### Region
- `classify_region()` using EU country/city maps; BA API defaults to `Germany`.

### 4.6 Validation Rules

| Stage | Rule |
|-------|------|
| Silver `validate_jobs` | `title`, `company`, `url` must be non-empty strings |
| Company careers | Skip if `_is_europe_job` fails |
| EURES | Skip items without `id` or `title` after normalization |
| SCD quality gate | Abort small Bronze batch against large Silver |

### 4.7 Error Handling Patterns

| Component | Pattern |
|-----------|---------|
| Ingestors | try/except → HTTP 500 JSON `{"error": message}` |
| Transformer | Re-raise Silver read failures; abort on quality anomaly |
| Gold generator | try/except → HTTP 500; continues if optional CSV missing in metrics |
| Fetchers | Per-item try/except with warning logs |
| GitHub redeploy | Silent skip if `GITHUB_TOKEN` missing |

---

## 5. KPIs, Metrics, and Calculations

### 5.1 Dashboard KPIs (`docs/dashboard.html`)

#### Total Jobs
- **Definition:** Count of active jobs in `all_jobs.csv`
- **Formula:** `len(all_jobs)` in `metrics_api._build_payload`
- **Source:** Gold `all_jobs.csv` (from Silver `is_current=True`)
- **Display:** KPI card "Total Jobs"
- **Update:** On Gold generator run (~3× daily + after EURES)

#### New Since Last Run
- **Definition:** Incremental new job IDs from the latest Silver pipeline run (`pipeline_stats.new_jobs`)
- **Fallback:** `sum(1 for j in all_jobs if j.date_added == today)` when pipeline stats are unavailable
- **Display:** KPI card "New Since Last Run" (avoids misleading total after full re-ingest snapshots)

#### Jobs by Source
- **Definition:** Active job count grouped by `source` field
- **Formula:** `current.groupby("source").size()` in Gold generator
- **Display:** Horizontal bar chart on dashboard; filter on job board
- **Example values:** `ba_api: 6626`, `eures: 1200`, `direct: 2178`

#### Jobs by Region
- **Definition:** Active jobs grouped by `region` (country / Remote / Other)
- **Formula:** `current.groupby("region").size()`
- **Display:** Doughnut chart (top 4 regions + Other)

#### Remote vs On-site
- **Definition:** Remote/on-site split for sources with reliable `remote` flag
- **Scope:** **Only** `arbeitnow` and `direct` sources
- **Formula:** `remote=True` → "Remote", else "On-site"; groupby count
- **Display:** Doughnut chart
- **Note:** BA and EURES excluded (unreliable remote signal)

#### Top Skills
- **Definition:** Frequency of 40+ predefined tech keywords in title+tags+description
- **Formula:** Regex `\b(Python|SQL|AWS|...)\b` case-insensitive on first 500 chars of description; `Counter.most_common(20)`
- **Display:** Bar chart (top skills)

#### Jobs Trend (30 days)
- **Definition:** Count of **first appearances** per calendar day
- **Formula:** `first_seen = df.sort_by(scd_start_date).drop_duplicates(job_id, keep='first')`; groupby date
- **Display:** Chart.js line chart (last 30 days from CSV)

#### Top Companies / Top Locations
- **Definition:** Top 20 employers / cities by active posting count
- **Location cleaning:** Take substring before first comma (`"Berlin, Germany"` → `"Berlin"`)
- **Display:** Top 5 companies list; location doughnut uses top 10

#### Description Insights (Arbeitnow-only)
| Metric | Logic |
|--------|-------|
| `english_jobs` | EN word ratio > 4% in first 1000 chars of description |
| `homeoffice_mentioned` | Regex match for remote/hybrid/home office keywords |
| `jobs_with_benefits` | HTML `<h2>` section titled Benefits/Vorteile |
| `arbeitnow_total` | Count of active Arbeitnow jobs |

#### Pipeline Stats
- **Source:** `pipeline_stats.csv` written by Silver transformer
- **Fields:** `new_jobs`, `updated_jobs`, `unchanged_jobs`, `total_silver`, `run_at`

### 5.2 Job Intelligence Board KPIs (`jobs_api` response)

| KPI | Meaning |
|-----|---------|
| `total` | All jobs in cached CSV (unfiltered) |
| `filtered` | Count after query filters |
| `returned` | Jobs in current response page |
| `truncated` | `True` if more results exist beyond offset+limit |
| `new_today` | Jobs added today |
| `remote` | Jobs with `is_remote=true` |

### 5.3 Agent Match Score (`agent.html`)

```
score = 0.6 × (matched_skills / total_user_skills)
      + 0.2 × seniority_match_bonus
      + 0.2 × location_match_bonus
```

Mapped to tiers: Strong Match (≥75%), Stretch (50–74%), Safety (<50%).

---

## 6. Data Storage Design (Lakehouse Schema)

### 6.1 Why S3 Lakehouse Instead of RDBMS

| Factor | S3 Lakehouse | PostgreSQL |
|--------|--------------|------------|
| Cost at scale | Pennies/GB | Instance always-on |
| Schema flexibility | Parquet evolution | Migration overhead |
| Analytics | Columnar Parquet + CSV | SQL joins |
| Serverless fit | Native Lambda integration | Connection pooling issues |

### 6.2 Bronze Schema (Unified Target)

| Column | Type | Description |
|--------|------|-------------|
| `job_id` | string | Unique stable identifier |
| `title` | string | Job title |
| `company` | string | Employer name |
| `location` | string | City/region text |
| `url` | string | Apply/detail URL |
| `description` | string | Plain or HTML text |
| `remote` | bool | Remote flag |
| `tags` | string | Comma-separated tags |
| `job_types` | string | Employment type |
| `source` | string | `ba_api`, `eures`, `arbeitnow`, etc. |
| `ingested_at` | string | ISO UTC ingestion timestamp |

**Partitioning:** `s3://bronze/{source}/ingested_at={YYYY-MM-DD}/jobs.parquet`

### 6.3 Silver Schema (SCD Type 2)

Bronze columns plus:

| Column | Description |
|--------|-------------|
| `hash_key` | SHA-256 of change-detection attributes |
| `scd_start_date` | Version start timestamp |
| `scd_end_date` | Version end (null if current) |
| `is_current` | Active flag |

**Partitioning:**
- `s3://silver/cleaned/jobs_history.parquet/is_current=True/`
- `s3://silver/cleaned/jobs_history.parquet/is_current=False/`

**Logical primary key:** `job_id` (semantic key after dedup)  
**Version key:** (`job_id`, `scd_start_date`)

### 6.4 Gold Schema (CSV Files)

See Section 3.7. Gold is **denormalized analytics export**, not normalized relational tables.

**`all_jobs.csv` key columns (Jobs API):**
`job_id`, `title`, `company`, `location`, `source`, `job_url`, `is_remote`, `date_added`, `tags`, `job_types`, `work_style`, `language_requirement`, `region`, `description` (truncated to 300 chars)

### 6.5 Data Normalization Strategy

1. **Ingest:** Source-specific → unified Bronze schema
2. **Silver:** Semantic dedup + SCD history (no duplicate active versions)
3. **Gold:** Enrichment columns derived from text analysis
4. **API:** Rename for frontend (`url`→`job_url`, `remote`→`is_remote`)

---

## 7. API Documentation

### 7.1 Metrics API

**Base URL:** `https://2aww80hwgj.execute-api.eu-central-1.amazonaws.com/`  
**Lambda:** `dataforge-metrics`  
**Methods:** `GET`, `OPTIONS`

#### Request
No required parameters.

#### Response `200 OK`
```json
{
  "total_jobs": 10383,
  "new_today": 42,
  "jobs_by_source": { "ba_api": 6626, "eures": 1200, ... },
  "jobs_by_region": { "Germany": 4500, "Remote": 800, ... },
  "trend": [{ "date": "2026-06-01", "count": 120 }, ...],
  "top_locations": [{ "location": "Berlin", "count": 450 }, ...],
  "top_companies": [{ "company": "Zalando", "count": 85 }, ...],
  "remote_vs_onsite": { "Remote": 1200, "On-site": 3400 },
  "active_vs_expired": { "Active": 10383, "Expired": 5200 },
  "top_skills": [{ "skill": "Python", "count": 2100 }, ...],
  "description_insights": {
    "english_jobs": 180,
    "homeoffice_mentioned": 95,
    "jobs_with_benefits": 40,
    "arbeitnow_total": 332
  },
  "pipeline_stats": { "new_jobs": 5, "updated_jobs": 12, "run_at": "..." },
  "last_updated": "2026-06-08T17:00:00Z"
}
```

#### Internal flow
1. Check 120s in-memory cache
2. Read 10+ CSVs from `GOLD_BUCKET`
3. Transform rows to dicts/arrays
4. Return JSON with CORS headers

#### Error cases
- Missing CSV → may throw (pipeline_stats optional with try/except)
- S3 permission failure → Lambda error → API Gateway 502

---

### 7.2 Jobs API

**Base URL:** `https://2amv4immb0.execute-api.eu-central-1.amazonaws.com/`  
**Lambda:** `dataforge-jobs-api`  
**Methods:** `GET`, `OPTIONS`

#### Query Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `search` | string | `""` | Substring in title, company, tags, location |
| `source` | string | `""` | Exact match: `ba_api`, `eures`, `arbeitnow`, `direct`, `berlin_startups` |
| `remote` | bool string | `""` | `true` or `false` on `is_remote` |
| `job_type` | string | `""` | Substring in `job_types` |
| `location` | string | `""` | Substring in `location` |
| `experience` | string | `""` | `junior`, `student`, `intern`, `thesis` → tag patterns |
| `language_req` | string | `""` | `english_only` |
| `work_style` | string | `""` | `remote`, `hybrid`, `onsite` |
| `region` | string | `""` | Exact match on `region` |
| `sort` | string | `newest` | `newest` or `oldest` by `date_added` |
| `status` | string | `active` | `active` → `all_jobs.csv`; `expired` → `expired_jobs.csv` |
| `limit` | int | `500` | Max **12000** |
| `offset` | int | `0` | Pagination offset |

#### Response `200 OK`
```json
{
  "jobs": [ { "job_id": "...", "title": "...", "job_url": "...", ... } ],
  "kpis": {
    "total": 10383,
    "new_today": 42,
    "remote": 2100,
    "filtered": 1200,
    "returned": 1200,
    "truncated": false,
    "offset": 0,
    "limit": 12000
  },
  "cached_at": 0
}
```

#### Internal flow
1. Load CSV from S3 (5-minute cache per status)
2. Apply filters sequentially (AND logic)
3. Sort by `date_added`
4. Slice `[offset:offset+limit]`
5. Compute KPIs on filtered vs full dataset

---

## 8. Frontend Logic

### 8.1 Component Structure

All frontends are **single-page HTML files** with embedded CSS and JavaScript (no React/Vue build step).

| File | Role |
|------|------|
| `index.html` | Landing page (Job Intelligence Platform) |
| `dashboard.html` | Job Intelligence Dashboard |
| `jobs.html` | Job Intelligence Board |
| `agent.html` | Career Matching Wizard |

### 8.2 State Management

**No global framework.** State held in JavaScript module variables:

```javascript
// jobs.html
let allJobs = [];      // Full fetched dataset
let filtered = [];     // After client-side filters
let page = 1;          // Pagination
```

`index.html` holds Chart.js instances for destroy/recreate on refresh.

### 8.3 Data Fetching Flow (`jobs.html`)

1. `loadJobs()` on source/work_style/language change
2. `fetchAllJobPages()` — loops API with `limit=2000`, `offset+=batch.length` until `truncated=false`
3. `populateCountryDropdown(allJobs)` — builds region `<select>` options
4. `applyFilters()` — client-side search, city, region, experience filters
5. `renderJobs()` — renders 25 cards, pagination controls

### 8.4 User Interaction Flow

**Job Intelligence Board:**
```
Page load → loadJobs() → fetch API → applyFilters() → renderJobs()
User changes source → reset filters → loadJobs()
User types search → applyFilters() only (no re-fetch)
User clicks Apply → opens job_url in new tab (EURES URLs fixed client-side)
```

**Dashboard:**
```
Page load → fetchMetrics() → render KPIs + charts
Every 2 min → fetchMetrics() again
```

**Agent:**
```
Step 1: paste resume → extractSkills()
Step 2: dream role → stored
Step 3: location → stored
Step 4: fetch jobs → score each → sort → render cards + table → CSV export
```

---

## 9. Authentication and Security

### 9.1 User Authentication

**None.** The system is intentionally public-read for dashboard and job search. There is no login, signup, session, or JWT for end users.

### 9.2 AWS Security

| Mechanism | Implementation |
|-----------|----------------|
| IAM roles | Least-privilege per Lambda (S3 scoped to project buckets) |
| S3 | Public access blocked; all access via IAM |
| SSM | API keys stored as String parameters (not SecureString — cost optimization) |
| Encryption | S3 SSE-S3 (AES256) |
| DLQ | SQS dead-letter queues on all Lambdas |
| Alarms | CloudWatch → SNS email on transformer, gold, metrics, company ingestor errors |
| API throttling | Burst 5, rate 3 req/s on API Gateway |

### 9.3 Application Security

| Topic | Status |
|-------|--------|
| CORS | `ALLOWED_ORIGIN` env (metrics); `*` default (jobs API) |
| Input validation | Query params sanitized via string lower/strip; no SQL (CSV read only) |
| XSS | `esc()` HTML-escapes rendered text; `escUrl()` for hrefs |
| Secrets in repo | `.gitignore` blocks `.env`, `*.tfvars`, `aws-keys-do-not-commit.txt` |

### 9.4 Authorization Rules

- **Public:** `GET` on metrics and jobs APIs
- **AWS-only:** S3 writes restricted to Lambda roles
- **GitHub Actions:** Repository secrets for AWS credentials on EURES/publish workflows

---

## 10. Technology Stack Justification

| Technology | Why chosen | Alternatives considered | Trade-off |
|------------|------------|-------------------------|-----------|
| **AWS Lambda** | Pay-per-invocation; no servers | EC2, ECS | 15-min timeout; cold starts |
| **S3** | Cheap durable storage; native Lambda integration | RDS, DynamoDB | No SQL joins; app-side filtering |
| **Parquet** | Columnar compression; awswrangler support | JSON Lines | Schema evolution manual |
| **Pandas** | Fast transforms in Lambda layer | Polars, Spark | Memory limits at scale |
| **Terraform** | Reproducible IaC; remote state | CDK, CloudFormation | Learning curve |
| **GitHub Pages** | Free static hosting | S3 static website | No server-side rendering |
| **API Gateway HTTP API** | Lower cost than REST API | ALB + Lambda | 10MB response limit |
| **GitHub Actions (EURES)** | Avoids Lambda WAF/timeout | Lambda with proxy | External CI dependency |
| **Chart.js** | Lightweight charting | D3, Plotly | CDN dependency |
| **pytest + moto** | Standard Python testing | unittest only | Mock fidelity limits |
| **DuckDB (scripts)** | Local SQL on CSVs without server | SQLite | Dev-only tool |

**Cost design:** SSM String (not SecureString), no Glue/Athena/Redshift, Lambda layer for heavy deps, GitHub Pages instead of CloudFront origin.

---

## 11. End-to-End System Flow

### 11.1 Scheduled Daily Run (Typical Day)

| UTC Time | Event |
|----------|-------|
| 04:00 | GitHub Action: EURES ingest → Bronze → invoke transformer |
| 07:00 | Lambda ingestors: Arbeitnow, BA, Direct, Berlin |
| 04:00 / 20:00 | GitHub Action: EURES scraper → Bronze |
| 07:30 | Transformer: Bronze → Silver SCD |
| 07:30+ | S3 trigger: Gold generator → 12 CSVs |
| 08:30 | `publish_gold.yml`: download CSVs → commit `data/gold/` |
| 12:00 / 16:00 | Repeat ingest + transform cycles |

### 11.2 User Views Dashboard

```
User browser
  → GET index.html (GitHub Pages CDN)
  → JavaScript fetch METRICS_API_URL
  → API Gateway → metrics Lambda
  → S3 read all Gold CSVs
  → JSON response
  → Chart.js renders trend, doughnuts, bars
```

### 11.3 User Searches EURES Jobs

```
User selects EURES filter
  → jobs.html fetchAllJobPages(source=eures)
  → Jobs API (4× paginated requests if ~1200 jobs)
  → Filtered JSON
  → Client render 25/page
  → User clicks Apply
  → Opens europa.eu/eures/portal/jv-details/{id}
```

---

## 12. Edge Cases and Error Handling

| Scenario | Handling |
|----------|----------|
| Empty Bronze day | Transformer exits 200 "No Bronze files found" |
| Small Bronze batch vs large Silver | `DATA_QUALITY_ANOMALY` exception — abort to protect Silver |
| EURES API retry | `AntigravityClient` 3 retries with exponential backoff |
| Silver Parquet read corruption | Warning per file; continue other files |
| Gold CSV missing in metrics | `pipeline_stats` optional; others required |
| API response > 10MB | Mitigated by pagination (2000 jobs/page) |
| Stale browser cache | `BOARD_BUILD` version + `Cache-Control: no-store` on jobs API |
| Duplicate job across sources | Semantic dedup in Bronze; SCD tracks one `job_id` |
| Job expires | Silver sets `is_current=False`; appears in `expired_jobs.csv` |
| Terraform Lambda zip | Built from `src/` including handlers; vendor deps bundled at deploy |
| GitHub redeploy without token | Logged skip; `publish_gold.yml` still updates repo snapshots |

---

## 13. Future Improvements

### 13.1 Scalability
- Migrate Gold serving to **Athena** or **DuckDB on S3** for SQL queries beyond CSV size limits
- **Step Functions** orchestration replacing chained cron assumptions
- **SQS queue** between ingest and transform for backpressure
- **Increase Arbeitnow/EURES coverage** (pagination, more keywords)

### 13.2 Performance
- **CloudFront** in front of API Gateway with short TTL
- **Precomputed filter indexes** in Gold (e.g., Parquet by source)
- **Lambda memory tuning** profiling for transformer/gold

### 13.3 Features
- Email alerts for new jobs matching saved searches
- Enhanced matching backend for Career Matching Wizard (optional)
- Salary extraction and compensation analytics
- Multi-language UI (DE/EN)
- OAuth admin panel for ingestor configuration

### 13.4 Data Quality
- Use `validate_ba_response` in BA fetch path
- Expand direct ATS company registry coverage
- Automated data quality tests in CI (row counts, null rates)
- Great Expectations or similar validation framework

### 13.5 DevOps
- Expand CI to run all unit tests (not just 3 modules)
- Terraform environments (`staging`, `prod`)
- Secrets Manager for production API keys
- Remove duplicate `pipeline_stats` assignment in `metrics_api.py`

---

## Appendix A — Source Identifier Reference

| `source` value | Origin | Typical volume |
|----------------|--------|----------------|
| `ba_api` | Bundesagentur für Arbeit | ~6,000+ |
| `direct` | Company career ATS feeds | ~2,000+ |
| `eures` | EU EURES portal | ~1,200 |
| `arbeitnow` | Arbeitnow API (2 pages) | ~300 |
| `berlin_startups` | Berlin RSS | ~15 |

---

## Appendix B — Environment Variables Reference

| Variable | Used by |
|----------|---------|
| `BRONZE_BUCKET` | All ingestors, transformer |
| `SILVER_PATH` | Transformer, gold generator |
| `GOLD_BUCKET` | Transformer stats, gold, APIs |
| `LOCAL_RUN` | `save_parquet` local mode |
| `ALLOWED_ORIGIN` | Metrics API CORS |
| `GOLD_KEY` | Jobs API (default `all_jobs.csv`) |
| `EURES_MAX_PAGES_PER_KEYWORD` | EURES ingestor |
| `COMPANY_CAREERS_*` | Company careers ingestor |
| `GITHUB_TOKEN`, `GITHUB_OWNER`, `GITHUB_REPO` | Gold generator redeploy |
---

*End of document.*

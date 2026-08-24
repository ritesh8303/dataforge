# DataForge

**DataForge** is a **Job Intelligence Platform** that aggregates, processes, and analyzes 10,000+ jobs across Europe using a multi-source ETL pipeline.

A serverless Data Lakehouse built on AWS for processing and analyzing German and European job market vacancies — delivered through a Job Intelligence Dashboard, Job Intelligence Board, and Career Matching Wizard.

## Architecture

```
EventBridge (daily at 20:00 UTC ≈ 22:00 CEST)
  ├── dataforge-ingestor        → Arbeitnow API (paginated, ~900 jobs)
  ├── dataforge-ba-ingestor     → BA Jobsuche API (multi-query, ~5000 jobs)
  ├── dataforge-company-ingestor→ Direct ATS career feeds (configurable companies)
  ├── dataforge-berlin-startups-ingestor → Berlin Startup Jobs RSS
  └── GitHub Action (04:00 + 19:15 UTC) → EURES EU job portal API
            │
            ▼ S3 upload (.parquet)
      dataforge-bronze-dev-eu-central-1 (Bronze Bucket, 14-day expiry)
            │
            ▼ EventBridge transformer (daily at 20:30 UTC)
      dataforge-transformer     → SCD Type 2 → Silver Layer (.parquet, history retained)
            │
            ▼ S3 trigger (gold_trigger/ completion marker)
      dataforge-gold-generator  → 12 analytics CSVs + metrics.json
            │
            ▼ S3 upload & GitHub Actions publishing
      GitHub Pages Website      ← Static UI hosted from /docs
            │
            ▼ Dynamic API Requests
      AWS API Gateway           → dataforge-metrics & dataforge-jobs-api (Lambdas)
```

### Layers
- **Bronze** — Raw Parquet files partitioned by date, one file per source per day.
- **Silver** — Deduplicated, SCD Type 2 history tracking all job changes, creations, and expirations over time.
- **Gold** — 12 analytics-ready CSVs plus a `metrics.json` snapshot, refreshed automatically after every Silver update, pushed to S3 and compiled for visualization.

### Gold Outputs
| File | Description |
|---|---|
| `all_jobs.csv` | All active jobs with title, company, location, source, date (trimmed descriptions) |
| `expired_jobs.csv` | Historically expired jobs with duration dates (`date_added`, `date_expired`) |
| `top_locations.csv` | Top 20 European cities by job count (cleaned) |
| `top_companies.csv` | Top 20 hiring companies |
| `jobs_by_source.csv` | Active postings breakdown by ingestion source |
| `remote_vs_onsite.csv` | Remote / hybrid / on-site breakdown across all sources |
| `data_quality_report.csv` | Completeness, uniqueness, freshness, and schema validation metrics |
| `jobs_trend.csv` | True new jobs added per day based on their initial appearance date |
| `active_vs_expired.csv` | Database summary tracking active vs historically expired jobs |
| `top_skills.csv` | Top 20 extracted technical skills from tags and job descriptions |
| `description_insights.csv`| Stopword ratios, home office mentions, and benefit visibility statistics |
| `pipeline_stats.csv` | Ingestion metrics tracking new, updated, and unchanged jobs per run |

## Tech Stack
- **Infrastructure**: Terraform (IaC) — remote state on S3 + DynamoDB locking
- **Presentation**: **GitHub Pages** static site hosting (`/docs` directory) consuming serverless REST APIs
- **Compute**: AWS Lambda (Python 3.11) — 8 functions (4 ingestors, transformer, gold generator, 2 APIs) + EURES via GitHub Actions
- **API Entry**: AWS API Gateway (HTTP APIs) routing requests to the metrics and jobs search Lambdas
- **Storage**: AWS S3 — Bronze, Silver, Gold buckets
- **Data Processing**: Pandas, AWS SDK for Pandas (awswrangler)
- **Monitoring**: CloudWatch Alarms → SNS email, SQS DLQ on all Lambdas
- **CI/CD**: GitHub Actions (runs test suites, generates reports, deploys Pages)

## Data Sources
- **Arbeitnow** — Public German job board API, no auth required
- **BA Jobsuche** — Official Federal Employment Agency API, no auth required
  - Queries: Data Engineer, Data Scientist, Data Analyst, Machine Learning,
    Business Intelligence, Data Architect, MLOps, Analytics Engineer
- **Direct company feeds** — Public career-page feeds from Greenhouse, Lever,
  Ashby, Workable, SmartRecruiters, Recruitee, Personio XML, Workday CXS,
  Comeet, and Pinpoint.
- **Berlin Startup Jobs** — RSS feeds (engineering, product, internships)
- **EURES** — EU job mobility portal API (GitHub Actions at 04:00 and 19:15 UTC; the evening run starts early because GitHub cron can lag, guaranteeing data lands before the 20:30 transformer).

### Pipeline schedule (1 full run per day, to stay comfortably inside the free tier)

| UTC | Local (CEST) | What runs |
|-----|--------------|-----------|
| 04:00 | 06:00 | EURES ingest (GitHub Actions) |
| 19:15 | 21:15 | EURES ingest (GitHub Actions, early margin for cron lag) |
| 20:00 | **22:00** | All Lambda ingestors |
| 20:30 | 22:30 | Silver transformer → Gold (triggered by completion marker) |
| 21:00 | 23:00 | Gold CSV publish to GitHub (backup sync) |

Direct company targets can be supplied without changing code:

```json
{
  "companies": [
    {"company": "Example", "careers_url": "https://boards.greenhouse.io/example"},
    {"company": "Example EU", "careers_url": "https://jobs.eu.lever.co/example"},
    {"company": "Example Workday", "careers_url": "https://example.wd3.myworkdayjobs.com/External"}
  ]
}
```

Set `COMPANY_CAREERS_CONFIG_S3_URI=s3://bucket/company-careers.json` for a
large registry, or `COMPANY_CAREERS_CONFIG` for inline JSON. Use
`COMPANY_CAREERS_CONFIG_MODE=replace` when the registry should replace the
built-in seed list.

## Cost Efficiency
Runs entirely within the **AWS Free Tier**:
- SSM Parameter Store instead of Secrets Manager
- Lambda + Pandas (awswrangler) instead of Glue or Athena; DuckDB for local analysis
- S3 Standard within 5GB limits — Bronze auto-expires after 14 days; Silver keeps
  full SCD history (bounded by the transformer's inactive-retention purge)
- GitHub Actions authenticates to AWS via OIDC role assumption (no stored keys)

## Project Layout

```
dataforge/
├── src/           # Lambda handlers and shared processing code
├── terraform/     # AWS infrastructure (S3, Lambda, API Gateway)
├── docs/          # GitHub Pages UI (landing, dashboard, job board, Career Matching Wizard)
├── scripts/       # Local dev and ops tooling
├── data/gold/     # Committed Gold aggregate CSVs (refreshed by CI; large row-level CSVs stay in S3 only)
├── tests/         # Unit and integration tests
└── .github/       # CI, EURES scraper, Gold publish, Pages deploy
```

## Jobs API pagination

The live Jobs API returns paginated JSON (default up to 2,000 jobs per request). Single
responses above ~5,000 jobs can exceed AWS Lambda payload limits and return HTTP 500.
The GitHub Pages Job Intelligence Board loads the full dataset in chunks automatically; clients should
use `offset` and `limit` query parameters for complete results.

## Running Locally

```bash
# Download latest Gold CSVs from S3
python scripts/download_all.py

# Regenerate local dashboard PNG from data/gold/
python scripts/visualize_gold.py

# Run an ingestor locally (writes to data/bronze/)
python scripts/run_ingestor_local.py eures

# Local API server using data/gold/ CSVs
python scripts/run_local_api.py

# Backfill Silver from existing Bronze files in S3
python scripts/backfill_silver.py
```

## Infrastructure

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

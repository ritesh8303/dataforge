# dataforge

A serverless Data Lakehouse built on AWS for processing and analyzing German and European data job market vacancies.

## Architecture

```
EventBridge (Daily Schedules)
  ├── dataforge-ingestor        → Arbeitnow API (paginated, ~900 jobs)
  ├── dataforge-ba-ingestor     → BA Jobsuche API (8 queries, ~5000 jobs)
  ├── dataforge-company-ingestor→ Direct ATS career feeds (configurable companies)
  ├── dataforge-apify-ingestor  → Apify Indeed scraping runs
  ├── dataforge-hn-ingestor     → Hacker News jobstories + Who is Hiring
  ├── dataforge-berlin-startups-ingestor → Berlin Startup Jobs RSS
  └── GitHub Action (daily 4AM) → EURES EU job portal API
            │
            ▼ S3 upload (.parquet)
      dataforge-bronze-dev-eu-central-1 (Bronze Bucket)
            │
            ▼ EventBridge (cron(30 7,12,16 * * ? *))
      dataforge-transformer     → SCD Type 2 → Silver Layer (.parquet)
            │
            ▼ S3 trigger (ObjectCreated)
      dataforge-gold-generator  → 11 Gold CSVs (.csv)
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
- **Gold** — 11 analytics-ready CSVs refreshed automatically after every Silver update, pushed to S3 and compiled for visualization.

### Gold Outputs
| File | Description |
|---|---|
| `all_jobs.csv` | All active jobs with title, company, location, source, date (trimmed descriptions) |
| `expired_jobs.csv` | Historically expired jobs with duration dates (`date_added`, `date_expired`) |
| `top_locations.csv` | Top 20 European cities by job count (cleaned) |
| `top_companies.csv` | Top 20 hiring companies |
| `jobs_by_source.csv` | Active postings breakdown by ingestion source |
| `remote_vs_onsite.csv` | Remote vs on-site breakdown for sources with remote signals |
| `jobs_trend.csv` | True new jobs added per day based on their initial appearance date |
| `active_vs_expired.csv` | Database summary tracking active vs historically expired jobs |
| `top_skills.csv` | Top 20 extracted technical skills from tags and job descriptions |
| `description_insights.csv`| Stopword ratios, home office mentions, and benefit visibility statistics |
| `pipeline_stats.csv` | Ingestion metrics tracking new, updated, and unchanged jobs per run |

## Tech Stack
- **Infrastructure**: Terraform (IaC) — remote state on S3 + DynamoDB locking
- **Presentation**: **GitHub Pages** static site hosting (`/docs` directory) consuming serverless REST APIs
- **Compute**: AWS Lambda (Python 3.11) — 10 functions (6 ingestors, transformer, gold generator, 2 APIs) + EURES via GitHub Actions
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
- **Apify scraper runs** — Indeed search task scraping.
- **EURES** — EU job mobility portal API (ingested daily via GitHub Actions at 4 AM UTC; picked up by the Silver transformer on its next run).

### Apify Scrapers Integration

The pipeline integrates with **Apify** to scrape tech job listings from platforms that do not offer open public APIs (such as Indeed).

1. **Orchestration & Asynchronous Runs:**
   - In order to stay within Lambda execution timeout limits, the scraper runs are decoupled from ingestion.
   - When the `dataforge-apify-ingestor` Lambda is triggered daily, it makes a POST request to trigger a new run of the configured actor tasks on the Apify platform.
   - It then queries the history of runs to fetch and download the default dataset items from the *last completed successful run* (providing a 1-day lag buffer, ensuring fast Lambda execution).
2. **SSM Configuration:**
   - Apify credentials and tasks are configured in the SSM Parameter Store under the key `/dataforge/dev/apify_credentials` as a JSON object:
     ```json
     {
       "apify_token": "apify_api_your_token_here",
       "tasks": {
         "indeed": "task_id_for_indeed"
       }
     }
     ```
3. **Data Normalization & EU Filter:**
   - Scraped job postings vary significantly in schema. The ingestor normalizes different Indeed scraper formats to the unified schema.
   - All scraped jobs are validated through the strict EU location safety gate before writing to S3 Bronze to filter out any international remote postings.


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
- Lambda + DuckDB instead of Glue or Athena
- S3 Standard within 5GB limits

## Project Layout

```
dataforge/
├── src/           # Lambda handlers and shared processing code
├── terraform/     # AWS infrastructure (S3, Lambda, API Gateway)
├── docs/          # GitHub Pages dashboard and job board UI
├── scripts/       # Local dev and ops tooling
├── data/gold/     # Committed Gold CSV snapshots (refreshed by CI)
├── tests/         # Unit and integration tests
└── .github/       # CI, EURES scraper, Gold publish, Pages deploy
```

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

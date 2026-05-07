# dataforge

A serverless Data Lakehouse built on AWS for processing German data job market data.

## Architecture

```
8AM UTC Daily (EventBridge)
  ├── dataforge-ingestor        → Arbeitnow API (paginated, ~900 jobs)
  ├── dataforge-ba-ingestor     → BA Jobsuche API (8 queries, ~5000 jobs)
  │         ↓ S3 trigger (.parquet)
  ├── dataforge-transformer     → SCD Type 2 → Silver Layer
  │         ↓ S3 trigger (.parquet)
  └── dataforge-gold-generator  → 7 Gold CSVs (auto-refreshed)
```

### Layers
- **Bronze** — Raw Parquet files partitioned by date, one file per source per day
- **Silver** — Deduplicated, SCD Type 2 history tracking all job changes over time
- **Gold** — 7 analytics-ready CSVs refreshed automatically after every Silver update

### Gold Outputs
| File | Description |
|---|---|
| `all_jobs.csv` | All active jobs with title, company, location, source, date |
| `top_locations.csv` | Top 20 German cities by job count |
| `top_companies.csv` | Top 20 hiring companies |
| `jobs_by_source.csv` | Arbeitnow vs BA API breakdown |
| `remote_vs_onsite.csv` | Remote vs on-site (Arbeitnow only) |
| `jobs_trend.csv` | New jobs added per day |
| `active_vs_expired.csv` | Active vs historically expired jobs |

## Tech Stack
- **Infrastructure**: Terraform (IaC) — remote state on S3 + DynamoDB locking
- **Compute**: AWS Lambda (Python 3.11) — 4 functions
- **Storage**: AWS S3 — Bronze, Silver, Gold buckets
- **Data Processing**: Pandas, AWS SDK for Pandas (awswrangler), Pydantic
- **Monitoring**: CloudWatch Alarms → SNS email, SQS DLQ on all Lambdas
- **CI**: GitHub Actions

## Data Sources
- **Arbeitnow** — Public German job board API, no auth required
- **BA Jobsuche** — Official Federal Employment Agency API, no auth required
  - Queries: Data Engineer, Data Scientist, Data Analyst, Machine Learning,
    Business Intelligence, Data Architect, MLOps, Analytics Engineer

## Cost Efficiency
Runs entirely within the **AWS Free Tier**:
- SSM Parameter Store instead of Secrets Manager
- Lambda + DuckDB instead of Glue or Athena
- S3 Standard within 5GB limits

## Running Locally

```bash
# Refresh gold CSVs from Silver
python analytics/query_gold.py

# Generate dashboard visualization
python analytics/visualize_gold.py

# Run E2E pipeline health check
set PYTHONIOENCODING=utf-8
python tests/test_e2e_pipeline.py

# Backfill Silver from all existing Bronze files
python analytics/backfill_silver.py
```

## Infrastructure

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

# DataForge

Live European **job-intelligence lakehouse**: five sources → medallion on AWS → Gold analytics → APIs → GitHub Pages.

**Live:** [Dashboard](https://ritesh8303.github.io/dataforge/) · **Code:** this repo  
**Thesis:** UE Applied Sciences M.Sc. Data Science — [`docs/thesis/`](docs/thesis/)

> I run a production medallion pipeline (Bronze / Silver SCD2 / Gold) with ingest validation, quality metrics, Terraform, and CI. Analytics engineering (dbt on Gold) is in-repo. GenAI matching/enrichment is **integrating** — not the public production story yet.

| | |
|---|---|
| **Shipped** | Multi-source ETL, Pydantic gates, SCD Type 2, Gold CSVs + quality report, Terraform, GitHub Actions, Pages UI, Jobs/Metrics APIs |
| **In this repo now** | Layer contracts, data dictionary, Gold quality gate in CI, **real dbt** (DuckDB) on Gold aggregates |
| **Integrating (in repo, not AWS-applied)** | `src/ai_gateway/`, enrichment, match API, `evals/`, `terraform/ai.tf` — do not CV as deployed |
| **Planned** | Persisted vector DB, LangGraph agent, honest eval report, full Docker lakehouse demo, EU AI Act 1-pager |

Details: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) · [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md) · [`docs/ROADMAP.md`](docs/ROADMAP.md)

## Architecture

```
EventBridge (daily at 20:00 UTC ≈ 22:00 CEST)
  ├── dataforge-ingestor        → Arbeitnow API
  ├── dataforge-ba-ingestor     → BA Jobsuche API
  ├── dataforge-company-ingestor→ Direct ATS career feeds
  ├── dataforge-berlin-startups-ingestor → Berlin Startup Jobs RSS
  └── GitHub Action (04:00 + 19:15 UTC) → EURES
            │
            ▼ S3 Parquet
      Bronze (14-day expiry) → Silver SCD Type 2 → Gold CSVs
            │
            ▼
      GitHub Pages (docs/)  +  API Gateway (metrics + jobs search)
```

### Layers

- **Bronze** — raw Parquet, one file per source per day.
- **Silver** — deduplicated SCD Type 2 history.
- **Gold** — analytics CSVs + `metrics.json`. Row-level `all_jobs` stays in S3; small aggregates are committed under `data/gold/` for dbt/CI.

## Tech stack

- **IaC:** Terraform (S3 backend + DynamoDB lock)
- **Compute:** AWS Lambda (Python 3.11) + EURES via GitHub Actions
- **Storage:** S3 Bronze / Silver / Gold (`eu-central-1`)
- **Analytics engineering:** dbt-core + DuckDB on Gold snapshots (same marts; warehouse adapter later)
- **CI:** Ruff, Pytest, Terraform validate, quality gate, `dbt run && dbt test`
- **UI:** GitHub Pages (`docs/`)

## Data sources

Arbeitnow · BA Jobsuche · company ATS feeds (Greenhouse, Lever, Ashby, Workable, SmartRecruiters, Recruitee, Personio, Workday, Comeet, Pinpoint) · Berlin Startup Jobs RSS · EURES.

## Local — quality + dbt (industry DE slice)

From the repo root (Python 3.11):

```bash
pip install -r requirements-test.txt -r requirements-analytics.txt
python scripts/check_quality_gate.py
dbt run --project-dir dbt --profiles-dir dbt
dbt test --project-dir dbt --profiles-dir dbt
pytest tests/ --ignore=tests/test_e2e_pipeline.py
```

Docker (Gold analytics only — not the AWS pipeline):

```bash
docker compose run --rm analytics
```

## Local — pipeline tooling

```bash
python scripts/download_all.py              # Gold CSVs from S3
python scripts/run_ingestor_local.py eures
python scripts/run_local_api.py
```

## Infrastructure

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

`terraform/ai.tf` (match API / enrichment) is in git as WIP. It is **not applied** on AWS until you run `terraform apply` and can demo it. Do not list it as production.

## Layout

```
src/            Lambda handlers + processing (Pydantic, SCD, Gold, quality)
terraform/      AWS lakehouse
dbt/            Real dbt project on Gold aggregates (DuckDB)
docs/           Pages UI + architecture / dictionary / thesis exposé
scripts/        Local ops + quality gate
data/gold/      Committed Gold aggregates (not all_jobs)
tests/          Pytest (skip live AWS e2e in CI)
evals/          Thesis eval harness (local; expand before claiming metrics)
```

## Cost

Designed for AWS Free Tier: SSM not Secrets Manager, Lambda + Pandas instead of Glue, Bronze 14-day expiry, GitHub OIDC (no long-lived AWS keys in Actions).

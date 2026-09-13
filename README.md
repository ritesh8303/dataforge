# DataForge

Live European **job-intelligence lakehouse**: multi-source ingest → medallion on AWS → Gold analytics → APIs → GitHub Pages — with a **frugal GenAI match layer** in-repo (Bedrock-first, hybrid retrieval, multi-agent optional).

**Live:** [Dashboard](https://ritesh8303.github.io/dataforge/) · **Code:** this repo  
**Thesis:** UE Applied Sciences M.Sc. Data Science — [`docs/thesis/`](docs/thesis/)

> Production AWS today: medallion ETL (Bronze / Silver SCD2 / Gold), Terraform, CI, Jobs/Metrics APIs, Pages UI, **Match Function URL** (FastAPI hybrid), enrichment Lambda (scheduled), Step Functions Express (Silver→Gold).  
> Set `MATCH_API_KEY` in tfvars when you want to lock the Match endpoint; currently open for portfolio demo under daily € budget + API GW throttle.

| | |
|---|---|
| **Shipped (AWS)** | Multi-source ETL, Pydantic gates, SCD Type 2, Gold CSVs + quality report, Terraform, GitHub Actions, Pages UI, Jobs/Metrics APIs, Match Function URL, enrichment Lambda, Step Functions Express |
| **Shipped (repo)** | dbt on Gold, hybrid BM25+dense+RRF, LanceDB helper, FastAPI Match+Mangum, PII, citations, visa filters, multi-agent graph, MCP, HITL, honest evals, local Airflow DAG, `docs/RESPONSIBLE_AI.md` |
| **Skip / thin** | Foundation training, K8s; Spark/streaming = design note |

Details: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) · [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md) · [`docs/ROADMAP.md`](docs/ROADMAP.md) · [`docs/RESPONSIBLE_AI.md`](docs/RESPONSIBLE_AI.md) · [`docs/DATA_SOURCING.md`](docs/DATA_SOURCING.md)

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
            ├── GitHub Pages (docs/)  +  API Gateway (metrics + jobs search)
            ├── Step Functions Express (Silver→Gold thin proof; schedule off by default)
            └── Enrichment + Match Function URL (FastAPI + multi-agent) — live
```

### Layers

- **Bronze** — raw Parquet, one file per source per day.
- **Silver** — deduplicated SCD Type 2 history (`is_tech`, rules fields).
- **Gold** — analytics CSVs + `metrics.json`. Row-level `all_jobs` stays in S3; small aggregates are committed under `data/gold/` for dbt/CI.

## Tech stack

- **IaC:** Terraform (S3 backend + DynamoDB lock); optional AI layer; Step Functions Express
- **Compute:** AWS Lambda (Python 3.11) + EURES via GitHub Actions
- **Storage:** S3 Bronze / Silver / Gold (`eu-central-1`); LanceDB-on-S3 helper for vectors
- **Match:** FastAPI + Mangum (local / container Lambda); hybrid retrieval; multi-agent (LangGraph or sequential)
- **Analytics:** dbt-core + DuckDB on Gold snapshots
- **Local DevX:** `docker compose` analytics; profiles `api` + `airflow`
- **CI:** Ruff, Pytest, Terraform validate, quality gate, `dbt run/test/docs`, matching evals
- **UI:** GitHub Pages (`docs/`) + `docs/agent.html`

## Data sources

Arbeitnow · BA Jobsuche · company ATS (Greenhouse, Lever, Ashby, Workable, SmartRecruiters, Recruitee, Personio, Workday, Comeet, Pinpoint) · Berlin Startup Jobs RSS · EURES · Himalayas · HN Who’s Hiring (see [`docs/DATA_SOURCING.md`](docs/DATA_SOURCING.md)).

## Local — quality + dbt (industry DE slice)

From the repo root (Python 3.11):

```bash
pip install -r requirements-test.txt -r requirements-analytics.txt
python scripts/check_quality_gate.py
dbt run --project-dir dbt --profiles-dir dbt
dbt test --project-dir dbt --profiles-dir dbt
pytest tests/ --ignore=tests/test_e2e_pipeline.py
```

Docker:

```bash
docker compose run --rm analytics          # quality + dbt + docs
docker compose --profile api up match-api-local
docker compose --profile airflow up airflow
```

Match API locally: `python scripts/run_match_api_local.py` then open `docs/agent.html` (hybrid / multi-agent modes).  
MCP: [`mcp/README.md`](mcp/README.md).

## Local — pipeline tooling

```bash
python scripts/download_all.py              # Gold CSVs from S3
python scripts/run_ingestor_local.py eures
python scripts/run_local_api.py
```

## Infrastructure

```bash
cd terraform
# AI layer is applied on EU (ai.tf present). Plan before further changes:
terraform init -reconfigure -backend-config=backends/eu.hcl
AWS_PROFILE=dataforge-germany terraform plan
```

**Live Match Function URL:** see `terraform/eu-outputs.json` → `match_function_url` (also wired in `docs/agent.html`).

See [`terraform/README.md`](terraform/README.md) and [`docs/BUDGET_RUNBOOK.md`](docs/BUDGET_RUNBOOK.md).

## Layout

```
src/            Lambda handlers + AI gateway + FastAPI Match + agent graph
terraform/      AWS lakehouse + stepfunctions; ai.tf.optional for Match/enrich
dbt/            Real dbt project on Gold aggregates (DuckDB)
docs/           Pages UI + architecture / dictionary / thesis / governance
mcp/            Stdio MCP bridge over Match/Jobs HTTPS
airflow/dags/   Local Docker Airflow proof (not MWAA)
scripts/        Local ops + quality gate
data/gold/      Committed Gold aggregates (not all_jobs)
tests/          Pytest (skip live AWS e2e in CI)
evals/          Thesis eval harness + ablation
```

## Cost

Designed for AWS Free Tier / €0–25 typical / €40 hard cap: SSM not Secrets Manager, Lambda + Pandas instead of Glue, Bronze 14-day expiry, GitHub OIDC, Match concurrency 2, Bedrock-first with kill switch. Spark/K8s/RDS/OpenSearch intentionally out of scope.

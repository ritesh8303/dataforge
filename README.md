# DataForge

Live European **early-career data & AI job lakehouse** on AWS Free Tier → Gold analytics → APIs → GitHub Pages, plus a **frugal GenAI Match** layer (hybrid BM25+dense, FastAPI Function URL).

**Live:** [Dashboard](https://ritesh8303.github.io/dataforge/) · [Job board](https://ritesh8303.github.io/dataforge/jobs.html) · [Match agent](https://ritesh8303.github.io/dataforge/agent.html)  
**Thesis:** UE Applied Sciences M.Sc. Data Science — [`docs/thesis/`](docs/thesis/)

### Impact (scanners)

- **~2.3k** product jobs (EU × data/AI × early-career) from **5** public sources; full lakehouse ~15k active for research — medallion Bronze → Silver SCD2 → Gold on AWS Lambda/S3
- **Match quality:** dense nDCG@10 **0.217** vs rule wizard **0.146** vs BM25 **0.135** (103 queries × 96 jobs; **synthetic** labels — Match is a suggested shortlist)
- **Unit cost:** modelled OpenAI-first enrichment ≈ **€0.10 / 1k jobs**; live RQ2 `gpt-4o-mini` 40/40 ≈ **$0.0011**
- **Trust:** product-first KPIs, source/freshness badges, link-health probe, stale SLA (14d), seeker feedback

### Architecture (1 page)

```
Public APIs / ATS / RSS ──► Bronze (raw Parquet, 14d TTL)
                         ──► Silver (SCD Type 2 history)
                         ──► Gold (CSVs + metrics.json)
                                ├── GitHub Pages UI + Metrics/Jobs APIs
                                └── Enrichment Lambda + Match Function URL (FastAPI)
```

| Layer | What | Consumers |
|---|---|---|
| Bronze | One raw file / source / day | Silver only |
| Silver | Deduped SCD2 (`job_id` versions) | Gold, audit |
| Gold | KPIs, quality report, search extract | Pages, APIs, dbt, Match |

**Hard nos:** no LinkedIn / Indeed / StepStone / Xing scrape; do not scrape workingstudentjobs.de ([`docs/DATA_SOURCING.md`](docs/DATA_SOURCING.md)).

| | |
|---|---|
| **Shipped (AWS)** | Multi-source ETL, Pydantic gates, SCD Type 2, Gold CSVs + quality report, Terraform, GitHub Actions, Pages UI, Jobs/Metrics APIs, Match Function URL, enrichment Lambda, Step Functions Express |
| **Shipped (repo)** | dbt on Gold, hybrid BM25+dense+RRF, LanceDB helper, FastAPI Match+Mangum, PII, citations, visa filters, multi-agent graph, MCP, HITL, honest evals, local Airflow DAG, `docs/RESPONSIBLE_AI.md` |
| **Skip / thin** | Foundation training, K8s; Spark/streaming = design note |

Details: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) · [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md) · [`docs/ROADMAP.md`](docs/ROADMAP.md) · [`docs/RESPONSIBLE_AI.md`](docs/RESPONSIBLE_AI.md) · [`docs/DATA_SOURCING.md`](docs/DATA_SOURCING.md)

### Match API auth

> Live Match `/match` and `/jobs` require **`X-API-Key`** (`match_api_key` in gitignored `terraform.tfvars` → Lambda `MATCH_API_KEY`). `/health` stays open. Enter the key in [agent.html](https://ritesh8303.github.io/dataforge/agent.html) (stored in localStorage) or pass `?api_key=…`. Leave the variable empty only for unlocked local demos. Daily € budget + API GW throttle still apply.

### Pipeline schedule

```
EventBridge (daily at 20:00 UTC ≈ 22:00 CEST)
  ├── dataforge-ingestor        → Arbeitnow API
  ├── dataforge-ba-ingestor     → BA Jobsuche API
  ├── dataforge-company-ingestor→ Direct ATS career feeds
  ├── dataforge-berlin-startups-ingestor → Berlin Startup Jobs RSS
  └── GitHub Action (04:00 + 19:15 UTC) → EURES
            │
            ▼ S3 Parquet → Bronze → Silver SCD2 → Gold
```

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

# DataForge architecture — layer contracts

**Shipped:** AWS medallion lakehouse for European job postings.  
**Integrating (local, not on `main` until reviewed):** LLM enrichment + embedding match.  
**Planned:** Docker one-command demo, persisted vector DB, LangGraph agent, honest eval report.

Do not describe integrating or planned items as live in CVs or interviews.

## Pipeline

```
5 sources
  → Bronze S3  (raw Parquet, 1 file / source / day, 14-day expiry)
  → Silver S3  (SCD Type 2 Parquet, job lifecycle)
  → Gold S3    (analytics CSVs + metrics.json)
  → API Gateway (metrics + jobs search)
  → GitHub Pages (docs/)
```

Schedule (CEST): EURES 06:00 + 21:15 · Lambda ingest 22:00 · transform 22:30 · Gold publish 23:00.

## Layer contracts

| Layer | Grain | Allowed | Forbidden | Consumers |
|---|---|---|---|---|
| **Bronze** | One source snapshot per run | Raw payload + `source`, `ingested_at`, `file_hash` equivalent (S3 key) | Business KPIs, PII enrichment beyond source fields | Silver transformer only |
| **Silver** | One `job_id` version (`scd_start_date`–`scd_end_date`, `is_current`) | Deduped, typed, SCD2 history | Dashboard-specific aggregations | Gold generator; audit queries |
| **Gold** | Aggregates + search extracts | KPIs, quality report, trimmed `all_jobs` | Re-implementing Silver SCD logic | Pages UI, Jobs/Metrics APIs, dbt marts, (later) agent |

## Quality gates

| Gate | Where | Fail means |
|---|---|---|
| Pydantic ingest models | Bronze Lambdas | Record dropped, not written |
| Region taxonomy | Gold (`validate_region_taxonomy`) | Gold job fails if `Remote` is a country |
| `scripts/check_quality_gate.py` | CI on committed Gold aggregates | Missing titles/companies, invalid sources, schema flag |
| dbt tests | CI `dbt test` | Nulls, non-unique keys, invalid `source` values |

## Local vs AWS

| Path | What it proves |
|---|---|
| AWS (`eu-central-1`) | Production ingest, SCD2, APIs, Pages |
| `dbt/` + DuckDB | Analytics engineering on Gold snapshots (same mart logic, adapter-swappable) |
| `scripts/run_*` | Replay pieces without deploying |

Full clone-and-run-in-under-2-minutes Docker for the **whole** AWS pipeline is **planned**, not shipped. Docker in this repo runs the **Gold analytics** path (quality gate + dbt).

# Data dictionary

Gold is the contract for BI, APIs, and (later) the matching agent. Row-level `all_jobs.csv` / `expired_jobs.csv` live in S3 only (too large for git). Aggregates in `data/gold/` are snapshots committed for dbt + CI.

## Entities

### Job (Silver grain)

| Field | Business definition | Source → Silver | Notes |
|---|---|---|---|
| `job_id` | Durable posting identity | Hash / source id per ingestor | Unique per version row with SCD dates |
| `title` | Job title | Source title | Required; Pydantic at ingest |
| `company` | Employer name | Source + `normalize_company` | Gold top-companies uses normalized form |
| `location` | Free-text location | Source | First comma segment used for top locations |
| `region` | Geographic country/region | Derived | Must **not** be `Remote` (work style is separate) |
| `source` | Ingest channel | `ba_api` \| `eures` \| `arbeitnow` \| `direct` \| `berlin_startups` | Closed set |
| `work_style` | remote / hybrid / onsite | Derived | Gold `remote_vs_onsite` |
| `url` / `job_url` | Apply link | Source | Renamed in Gold extract |
| `is_current` | Open SCD2 version | Transformer | Gold active vs expired |
| `scd_start_date` / `scd_end_date` | Version window | Transformer | First start date = trend “new jobs” |
| `ingested_at` | Landed in pipeline | Ingest | Freshness / stale counts |

### Gold marts (git + S3)

| File | Grain | Downstream |
|---|---|---|
| `jobs_by_source.csv` | `source` | Dashboard, dbt `mart_source_share` |
| `jobs_by_region.csv` | `region` | Dashboard map/table |
| `top_locations.csv` | cleaned city/country label | Dashboard |
| `top_companies.csv` | normalized company | Dashboard |
| `top_skills.csv` | skill keyword | Dashboard (regex, not LLM) |
| `remote_vs_onsite.csv` | `work_type` | Dashboard |
| `jobs_trend.csv` | `date` of first appearance | Dashboard |
| `active_vs_expired.csv` | status | Dashboard |
| `pipeline_stats.csv` | pipeline run | Ops / thesis evaluation window |
| `data_quality_report.csv` | one row per Gold run | CI quality gate, thesis metrics |
| `description_insights.csv` | one row | English / benefits heuristics |
| `all_jobs.csv` (S3) | active job | Jobs API, wizard, future RAG corpus |

### Quality report fields

| Field | Meaning |
|---|---|
| `total_jobs` | Active Silver rows in that Gold run |
| `missing_*_rate` | Empty company/title/location on active rows |
| `duplicate_job_id_rate` | Duplicate `job_id` among active |
| `stale_jobs_count` | `ingested_at` older than 7 days |
| `invalid_source_count` | Source outside the closed set |
| `remote_in_country_dimension` | `region == Remote` (must be 0) |
| `schema_validation_pass` | Required columns present and taxonomy OK |

## Known gaps (honest)

- Git `data_quality_report.csv` can lag live S3 Gold (snapshot vs daily publish).
- Skill tags are regex, not an ontology or LLM extraction, until enrichment is deployed.
- No PII-masking contract on resume upload yet (matching wizard is client-side today).
- dbt models the **Gold aggregates**, not a second copy of SCD2 logic.

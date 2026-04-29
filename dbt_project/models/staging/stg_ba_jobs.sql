-- staging/stg_ba_jobs.sql
-- Selects only current BA API records from Silver and standardizes the schema.
-- Bronze normalization already renamed refnr→job_id, titel→title, arbeitgeber→company.

with source as (
    select * from read_parquet(
        's3://dataforge-silver-dev-eu-central-1/cleaned/jobs_history.parquet',
        union_by_name = true
    )
),

current_records as (
    select * from source
    where is_current = true
      and source = 'ba_api'
),

renamed as (
    select
        job_id,
        title,
        company,
        location,
        source,
        -- BA-specific fields
        zip_code,
        state,
        start_date_raw,
        modified_at,
        -- Shared audit fields
        ingested_at,
        scd_start_date,
        hash_key,
        -- BA has no description/remote/url/tags — use nulls for unified schema
        null as description,
        null as remote,
        null as url,
        null as tags,
        null as job_types,
        null as created_at
    from current_records
)

select * from renamed

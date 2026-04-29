-- staging/stg_arbeitnow_jobs.sql
-- Selects only current Arbeitnow records from Silver and standardizes the schema.
-- Bronze normalization already renamed slug→job_id, company_name→company.

with source as (
    select * from read_parquet(
        's3://dataforge-silver-dev-eu-central-1/cleaned/jobs_history.parquet',
        union_by_name = true
    )
),

current_records as (
    select * from source
    where is_current = true
      and source = 'arbeitnow'
),

renamed as (
    select
        job_id,
        title,
        company,
        location,
        source,
        description,
        remote,
        url,
        tags,
        job_types,
        created_at,
        ingested_at,
        scd_start_date,
        hash_key
    from current_records
)

select * from renamed

-- intermediate/int_jobs_unified.sql
-- Unions Arbeitnow and BA staging models into a single clean table.
-- Only current records. Deduplicates on job_id keeping most recently ingested.

with arbeitnow as (
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
        ingested_at,
        scd_start_date,
        -- Arbeitnow has no zip_code/state
        null as zip_code,
        null as state
    from {{ ref('stg_arbeitnow_jobs') }}
),

ba as (
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
        ingested_at,
        scd_start_date,
        zip_code,
        state
    from {{ ref('stg_ba_jobs') }}
),

unioned as (
    select * from arbeitnow
    union all
    select * from ba
),

-- Deduplicate: if same job_id appears in both sources, keep most recently ingested
deduped as (
    select *,
        row_number() over (
            partition by job_id
            order by ingested_at desc
        ) as row_num
    from unioned
),

final as (
    select
        job_id,
        title,
        company,
        -- Normalize empty strings to NULL for cleaner analytics
        nullif(trim(location), '')  as location,
        nullif(trim(zip_code), '')  as zip_code,
        nullif(trim(state), '')     as state,
        source,
        description,
        remote,
        url,
        tags,
        job_types,
        ingested_at,
        scd_start_date
    from deduped
    where row_num = 1
)

select * from final

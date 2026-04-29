-- marts/mart_jobs_by_source.sql
-- Daily job posting counts broken down by source.
-- Used for time series charts showing pipeline activity and source comparison.

with jobs as (
    select
        source,
        cast(ingested_at as date)   as ingestion_date,
        job_id
    from {{ ref('int_jobs_unified') }}
),

daily_counts as (
    select
        ingestion_date,
        source,
        count(job_id)               as jobs_ingested
    from jobs
    group by ingestion_date, source
),

pivoted as (
    select
        ingestion_date,
        sum(case when source = 'arbeitnow' then jobs_ingested else 0 end) as arbeitnow_jobs,
        sum(case when source = 'ba_api'    then jobs_ingested else 0 end) as ba_jobs,
        sum(jobs_ingested)                                                 as total_jobs
    from daily_counts
    group by ingestion_date
)

select * from pivoted
order by ingestion_date desc

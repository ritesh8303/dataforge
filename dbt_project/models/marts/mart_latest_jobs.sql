-- marts/mart_latest_jobs.sql
-- Most recent 50 current job postings across both sources.
-- Used for the jobs table in the dashboard.

with jobs as (
    select
        job_id,
        title,
        company,
        coalesce(location, 'Remote / Unknown')  as location,
        source,
        case
            when source = 'arbeitnow' then 'Arbeitnow'
            when source = 'ba_api'    then 'Bundesagentur für Arbeit'
            else source
        end                                      as source_display,
        case
            when remote = true  then 'Remote'
            when remote = false then 'On-site'
            else 'Unknown'
        end                                      as work_type,
        tags,
        url,
        cast(ingested_at as date)                as ingested_date,
        scd_start_date
    from {{ ref('int_jobs_unified') }}
)

select * from jobs
order by scd_start_date desc
limit 50

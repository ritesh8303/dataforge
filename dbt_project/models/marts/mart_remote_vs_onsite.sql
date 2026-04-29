-- marts/mart_remote_vs_onsite.sql
-- Remote vs on-site job posting breakdown.
-- Only Arbeitnow provides the remote flag — BA API does not.
-- Used for pie charts and trend analysis in the dashboard.

with arbeitnow_jobs as (
    select
        job_id,
        title,
        company,
        location,
        remote,
        ingested_at,
        cast(ingested_at as date) as ingestion_date
    from {{ ref('int_jobs_unified') }}
    where source = 'arbeitnow'
      and remote is not null
),

summary as (
    select
        case when remote = true then 'Remote' else 'On-site' end as work_type,
        count(*)                                                   as job_count,
        round(count(*) * 100.0 / sum(count(*)) over (), 1)        as percentage
    from arbeitnow_jobs
    group by remote
),

daily_trend as (
    select
        ingestion_date,
        sum(case when remote = true  then 1 else 0 end) as remote_jobs,
        sum(case when remote = false then 1 else 0 end) as onsite_jobs,
        count(*)                                         as total_jobs,
        round(
            sum(case when remote = true then 1 else 0 end) * 100.0 / count(*),
            1
        )                                                as remote_pct
    from arbeitnow_jobs
    group by ingestion_date
)

select
    s.work_type,
    s.job_count,
    s.percentage,
    null as ingestion_date,
    null as remote_jobs,
    null as onsite_jobs,
    null as total_jobs,
    null as remote_pct,
    'summary' as record_type
from summary

union all

select
    null as work_type,
    null as job_count,
    null as percentage,
    d.ingestion_date,
    d.remote_jobs,
    d.onsite_jobs,
    d.total_jobs,
    d.remote_pct,
    'daily_trend' as record_type
from daily_trend
order by record_type, ingestion_date desc

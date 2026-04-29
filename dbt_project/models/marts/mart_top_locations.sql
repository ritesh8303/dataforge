-- marts/mart_top_locations.sql
-- Top German cities by number of current Data Engineer job postings.
-- Used for bar charts and location filters in the dashboard.

with jobs as (
    select * from {{ ref('int_jobs_unified') }}
    where location is not null
),

location_counts as (
    select
        location,
        count(*)                                    as job_count,
        count(case when source = 'arbeitnow' then 1 end) as arbeitnow_count,
        count(case when source = 'ba_api'    then 1 end) as ba_count,
        round(
            count(case when source = 'arbeitnow' then 1 end) * 100.0 / count(*),
            1
        )                                           as arbeitnow_pct
    from jobs
    group by location
),

ranked as (
    select
        location,
        job_count,
        arbeitnow_count,
        ba_count,
        arbeitnow_pct,
        rank() over (order by job_count desc) as location_rank
    from location_counts
)

select * from ranked
order by location_rank

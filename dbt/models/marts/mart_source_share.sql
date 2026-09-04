select
    source,
    job_count,
    round(job_count * 1.0 / sum(job_count) over (), 4) as source_share
from {{ ref('stg_jobs_by_source') }}

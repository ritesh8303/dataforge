select
    source::varchar as source,
    job_count::bigint as job_count
from {{ source('gold', 'jobs_by_source') }}

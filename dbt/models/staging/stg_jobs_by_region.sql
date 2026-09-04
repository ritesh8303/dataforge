select
    region::varchar as region,
    job_count::bigint as job_count
from {{ source('gold', 'jobs_by_region') }}

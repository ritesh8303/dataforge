select
    skill::varchar as skill,
    job_count::bigint as job_count
from {{ source('gold', 'top_skills') }}

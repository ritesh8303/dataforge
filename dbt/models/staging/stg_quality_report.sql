select
    total_jobs::bigint as total_jobs,
    missing_company_rate::double as missing_company_rate,
    missing_title_rate::double as missing_title_rate,
    missing_location_rate::double as missing_location_rate,
    duplicate_job_id_rate::double as duplicate_job_id_rate,
    stale_jobs_count::bigint as stale_jobs_count,
    invalid_source_count::bigint as invalid_source_count,
    remote_in_country_dimension::bigint as remote_in_country_dimension,
    schema_validation_pass as schema_validation_pass,
    computed_at::varchar as computed_at
from {{ source('gold', 'data_quality_report') }}

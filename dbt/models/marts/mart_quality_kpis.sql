select
    total_jobs,
    missing_title_rate,
    missing_company_rate,
    missing_location_rate,
    duplicate_job_id_rate,
    invalid_source_count,
    remote_in_country_dimension,
    schema_validation_pass,
    computed_at
from {{ ref('stg_quality_report') }}

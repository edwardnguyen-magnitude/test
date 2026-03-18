select
    pfp_id,
    trim(part_number) as part_number,
    trim(part_description) as part_description,
    trim(model_name) as model_name,
    cast(model_year as integer) as model_year,
    cast(vehicles_in_population as integer) as vehicles_in_population,
    cast(expected_failure_rate_per_1000 as numeric) as expected_failure_rate_per_1000,
    cast(actual_failure_count as integer) as actual_failure_count,
    cast(actual_failure_rate_per_1000 as numeric) as actual_failure_rate_per_1000,
    cast(deviation_pct as numeric) as deviation_pct,
    trim(trend) as trend,
    trim(period) as period,
    cast(last_updated as date) as last_updated

from {{ source('raw', 'ref_pfp') }}

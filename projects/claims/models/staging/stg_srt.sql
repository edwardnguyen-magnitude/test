select
    srt_id,
    lpad(cast(vmrs_component_code as text), 3, '0') as vmrs_component_code,
    lpad(cast(vmrs_cause_code as text), 2, '0') as vmrs_cause_code,
    trim(repair_type) as repair_type,
    cast(standard_labor_hours as numeric) as standard_labor_hours,
    cast(labor_hour_tolerance_pct as integer) as labor_hour_tolerance_pct,
    trim(applicable_models) as applicable_models,
    cast(effective_date as date) as effective_date,
    cast(last_updated as date) as last_updated

from {{ ref('ref_srt') }}

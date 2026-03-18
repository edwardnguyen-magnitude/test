select
    claim_line_id,
    claim_id,
    cast(line_number as integer) as line_number,
    trim(part_number) as part_number,
    lpad(cast(vmrs_component_code as text), 3, '0') as vmrs_component_code,
    lpad(cast(vmrs_cause_code as text), 2, '0') as vmrs_cause_code,
    lpad(cast(vmrs_correction_code as text), 2, '0') as vmrs_correction_code,
    cast(quantity as integer) as quantity,
    cast(unit_cost as numeric) as unit_cost,
    cast(line_total as numeric) as line_total,
    cast(labor_hours as numeric) as labor_hours,
    cast(srt_standard_hours as numeric) as srt_standard_hours,
    cast(labor_variance_pct as numeric) as labor_variance_pct,
    cast(cost_variance_pct as numeric) as cost_variance_pct,
    trim(repair_type) as repair_type

from {{ source('raw', 'fact_claim_lines') }}

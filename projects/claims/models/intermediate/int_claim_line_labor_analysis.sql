with claim_lines as (

    select * from {{ ref('stg_claim_lines') }}

),

srt as (

    select * from {{ ref('stg_srt') }}

),

enriched as (

    select
        cl.claim_line_id,
        cl.claim_id,
        cl.line_number,
        cl.part_number,
        cl.vmrs_component_code,
        cl.vmrs_cause_code,
        cl.vmrs_correction_code,
        cl.quantity,
        cl.unit_cost,
        cl.line_total,
        cl.labor_hours,
        cl.srt_standard_hours,
        cl.labor_variance_pct,
        cl.cost_variance_pct,
        cl.repair_type,

        -- SRT reference enrichment
        srt.standard_labor_hours as srt_reference_hours,
        srt.labor_hour_tolerance_pct as srt_tolerance_pct,
        srt.applicable_models,

        -- calculated: labor efficiency status
        case
            when abs(cl.labor_variance_pct) <= coalesce(srt.labor_hour_tolerance_pct, 20)
                then 'within_tolerance'
            when cl.labor_variance_pct > 0
                then 'over_standard'
            else 'under_standard'
        end as labor_efficiency_status,

        -- calculated: cost flag based on variance
        case
            when cl.cost_variance_pct > 5 then 'high'
            when cl.cost_variance_pct < -5 then 'low'
            else 'normal'
        end as cost_flag,

        -- calculated: absolute labor hours difference
        cl.labor_hours - cl.srt_standard_hours as labor_hours_delta

    from claim_lines cl
    left join srt
        on cl.vmrs_component_code = srt.vmrs_component_code
        and cl.vmrs_cause_code = srt.vmrs_cause_code
        and cl.repair_type = srt.repair_type

)

select * from enriched

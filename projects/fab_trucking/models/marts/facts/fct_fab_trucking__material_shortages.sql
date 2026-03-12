{{ config(materialized='table') }}

with shortage_impact as (
    select * from {{ ref('int_fab_trucking__shortage_impact') }}
),

final as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['shortage_id']) }} as shortage_key,
        shortage_id,
        truck_build_id,
        part_id,
        supplier_id,
        tenant_id,

        -- Shortage details
        qty_short as quantity_short,
        case
            when expected_arrival_ts is not null then expected_arrival_ts::date
            else null
        end as expected_arrival_date,
        days_until_arrival,
        shortage_detected_ts as shortage_reported_at,
        shortage_resolved_ts as shortage_resolved_at,
        shortage_status,
        shortage_age_days,

        -- Flags
        is_open_shortage,
        false as is_critical_part,

        -- Impact analysis
        severity_score,
        case
            when severity_score >= 80 then 'CRITICAL'
            when severity_score >= 50 then 'HIGH'
            when severity_score >= 20 then 'MEDIUM'
            else 'LOW'
        end as severity_band,

        -- Source
        source_doc_type as source_system,

        -- Timestamps
        created_at,
        updated_at

    from shortage_impact
)

select * from final

{{ config(materialized='table') }}

with defects as (
    select * from {{ ref('stg_fab_trucking__defects') }}
),

trucks as (
    select * from {{ ref('stg_fab_trucking__trucks') }}
),

plants as (
    select * from {{ ref('dim_fab_trucking__plants') }}
),

final as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['d.defect_id']) }} as defect_key,
        d.defect_id,
        d.vin,
        t.truck_build_id,
        d.tenant_id,

        -- Plant info
        p.plant_key,
        t.plant_id,
        p.plant_code,
        p.plant_name,

        -- Part/supplier info
        d.part_id,
        d.supplier_id,

        -- Defect details (canonical column names)
        d.defect_code,
        d.defect_description,
        d.defect_category,
        d.severity,
        d.detected_at_stage,

        -- Severity classification
        d.severity = 'CRITICAL' as is_critical,
        d.severity = 'MAJOR' as is_major,
        d.severity = 'MINOR' as is_minor,

        -- Status derived from resolved_ts
        case
            when d.resolved_ts is not null then 'RESOLVED'
            else 'OPEN'
        end as defect_status,
        (d.resolved_ts is null) as is_open,
        (d.resolved_ts is not null) as is_resolved,

        -- Timing (canonical column names)
        d.detected_ts,
        d.resolved_ts,
        d.detected_ts::date as detected_date,

        -- Repair duration
        case
            when d.resolved_ts is not null and d.detected_ts is not null
            then extract(epoch from (d.resolved_ts - d.detected_ts)) / 3600
            else null
        end as repair_duration_hours,

        -- Age calculation for open defects
        case
            when d.resolved_ts is null
            then extract(day from (current_timestamp - d.detected_ts))
            else null
        end as defect_age_days,

        -- Timestamps
        d.created_at

    from defects d
    left join trucks t on d.vin = t.vin
    left join plants p on t.plant_id = p.plant_id
)

select * from final

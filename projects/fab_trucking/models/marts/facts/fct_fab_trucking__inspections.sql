{{ config(materialized='table') }}

with inspections as (
    select * from {{ ref('stg_fab_trucking__inspections') }}
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
        {{ dbt_utils.generate_surrogate_key(['i.inspection_event_id']) }} as inspection_key,
        i.inspection_event_id,
        i.vin,
        t.truck_build_id,
        i.tenant_id,

        -- Plant info
        p.plant_key,
        i.plant_id,
        p.plant_code,
        p.plant_name,

        -- Workcenter info
        i.workcenter_id,

        -- Inspection details (canonical column names)
        i.inspection_type,
        i.inspector_employee_id,
        i.scheduled_ts,
        i.started_ts,
        i.completed_ts,
        i.completed_ts::date as inspection_date,

        -- Results
        i.inspection_result,
        i.inspection_result = 'PASS' as is_passed,
        i.inspection_result = 'FAIL' as is_failed,
        i.inspection_result = 'REWORK_REQUIRED' as is_rework_required,

        -- Defect counts
        i.defect_count,
        i.defect_severity_max,

        -- Source
        i.source_system,

        -- Timestamps
        i.created_at

    from inspections i
    left join trucks t on i.vin = t.vin
    left join plants p on i.plant_id = p.plant_id
)

select * from final

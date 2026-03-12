{{ config(materialized='table') }}

with paint_operations as (
    select * from {{ ref('stg_fab_trucking__paint_operations') }}
),

trucks as (
    select * from {{ ref('stg_fab_trucking__trucks') }}
),

plants as (
    select * from {{ ref('dim_fab_trucking__plants') }}
),

workcenters as (
    select * from {{ ref('dim_fab_trucking__workcenters') }}
),

final as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['po.paint_operation_id']) }} as paint_operation_key,
        po.paint_operation_id,
        po.vin,
        t.truck_build_id,
        po.tenant_id,

        -- Plant info
        p.plant_key,
        po.plant_id,
        p.plant_code,
        p.plant_name,

        -- Workcenter info
        w.workcenter_key,
        po.workcenter_id,
        w.workcenter_code,
        w.workcenter_name,

        -- Paint details (canonical column names)
        po.paint_code,
        po.requested_paint_ts,
        po.paint_start_ts,
        po.paint_end_ts,
        po.paint_end_ts::date as paint_date,

        -- Duration calculations
        extract(epoch from (po.paint_end_ts - po.paint_start_ts)) / 3600 as paint_duration_hours,
        extract(epoch from (po.paint_start_ts - po.requested_paint_ts)) / 3600 as wait_time_hours,

        -- Results
        po.paint_result,
        po.paint_result = 'OK' as is_ok,
        po.paint_result = 'REWORK_REQUIRED' as is_rework_required,
        po.rework_count,

        -- Source
        po.source_system,

        -- Timestamps
        po.created_at

    from paint_operations po
    left join trucks t on po.vin = t.vin
    left join plants p on po.plant_id = p.plant_id
    left join workcenters w on po.workcenter_id = w.workcenter_id
)

select * from final

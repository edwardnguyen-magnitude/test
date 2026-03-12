{{ config(materialized='view') }}

with source as (
    select * from {{ ref('paint_operations') }}
),

staged as (
    select
        -- Primary key
        paint_operation_id,

        -- Tenant
        tenant_id,

        -- Foreign keys
        vin,
        plant_id,
        workcenter_id,

        -- Paint attributes (matching canonical schema)
        paint_code,
        requested_paint_ts,
        paint_start_ts,
        paint_end_ts,
        paint_result,
        rework_count,
        source_system,

        -- Timestamps
        created_at

    from source
)

select * from staged

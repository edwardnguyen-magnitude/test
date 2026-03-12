{{ config(materialized='view') }}

with source as (
    select * from {{ ref('workcenters') }}
),

staged as (
    select
        -- Primary key
        workcenter_id,

        -- Tenant
        tenant_id,

        -- Foreign keys
        plant_id,

        -- Workcenter attributes
        code as workcenter_code,
        name as workcenter_name,
        area,
        capacity_units_per_shift::integer as capacity_units_per_shift,
        shift_pattern,
        status,

        -- Timestamps
        created_at::timestamp as created_at

    from source
)

select * from staged

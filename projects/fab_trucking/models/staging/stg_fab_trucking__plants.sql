{{ config(materialized='view') }}

with source as (
    select * from {{ ref('plants') }}
),

staged as (
    select
        -- Primary key
        plant_id,

        -- Tenant
        tenant_id,

        -- Plant attributes
        plant_code,
        plant_name,
        location_country,
        location_region,
        time_zone,
        capacity_trucks_per_day::integer as capacity_trucks_per_day,
        manager_employee_id,
        plant_type,
        status,

        -- Timestamps
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at

    from source
)

select * from staged

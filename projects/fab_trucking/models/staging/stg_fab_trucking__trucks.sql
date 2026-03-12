{{ config(materialized='view') }}

with source as (
    select * from {{ ref('trucks') }}
),

staged as (
    select
        -- Primary key
        truck_build_id,

        -- Tenant
        tenant_id,

        -- Natural key
        vin,

        -- Foreign keys
        plant_id,
        truck_model_id,
        configuration_id,
        customer_id,

        -- Build dates
        planned_build_date::date as planned_build_date,
        actual_build_start_ts::timestamp as actual_build_start_ts,
        actual_build_end_ts::timestamp as actual_build_end_ts,
        planned_ship_date::date as planned_ship_date,
        planned_customer_delivery_date::date as planned_customer_delivery_date,

        -- Status
        current_status,
        build_source_system,

        -- Timestamps
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at

    from source
)

select * from staged

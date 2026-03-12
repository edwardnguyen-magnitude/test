{{ config(materialized='view') }}

with source as (
    select * from {{ ref('truck_models') }}
),

staged as (
    select
        -- Primary key
        truck_model_id,

        -- Tenant
        tenant_id,

        -- Model attributes
        model_code,
        model_name,
        model_year::integer as model_year,
        segment,
        base_msrp::decimal(12,2) as base_msrp,

        -- Timestamps
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at

    from source
)

select * from staged

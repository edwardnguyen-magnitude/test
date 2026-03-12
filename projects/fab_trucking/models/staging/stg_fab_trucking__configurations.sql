{{ config(materialized='view') }}

with source as (
    select * from {{ ref('configurations') }}
),

staged as (
    select
        -- Primary key
        configuration_id,

        -- Tenant
        tenant_id,

        -- Foreign keys
        truck_model_id,

        -- Configuration attributes
        configuration_code,
        description,
        market,
        has_custom_engineering::boolean as has_custom_engineering,
        requires_special_test::boolean as requires_special_test,
        requires_custom_paint::boolean as requires_custom_paint,
        base_price_adjustment::decimal(12,2) as base_price_adjustment,

        -- Timestamps
        created_at::timestamp as created_at

    from source
)

select * from staged

{{ config(materialized='view') }}

with source as (
    select * from {{ ref('customers') }}
),

staged as (
    select
        -- Primary key
        customer_id,

        -- Tenant
        tenant_id,

        -- Customer attributes
        customer_code,
        name as customer_name,
        country,
        customer_segment,
        dealer_id,
        credit_limit::decimal(14,2) as credit_limit,
        payment_terms_days::integer as payment_terms_days,

        -- Timestamps
        created_at::timestamp as created_at,
        updated_at::timestamp as updated_at

    from source
)

select * from staged

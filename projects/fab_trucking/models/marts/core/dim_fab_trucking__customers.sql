{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_fab_trucking__customers') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
        customer_id,
        tenant_id,
        customer_code,
        customer_name,
        country,
        customer_segment,
        dealer_id,
        credit_limit,
        payment_terms_days,
        created_at,
        updated_at
    from customers
)

select * from final

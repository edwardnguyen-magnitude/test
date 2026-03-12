{{ config(materialized='table') }}

with configurations as (
    select * from {{ ref('stg_fab_trucking__configurations') }}
),

truck_models as (
    select * from {{ ref('stg_fab_trucking__truck_models') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['c.configuration_id']) }} as configuration_key,
        c.configuration_id,
        c.tenant_id,
        c.truck_model_id,
        tm.model_code,
        tm.model_name,
        c.configuration_code,
        c.description,
        c.market,
        c.has_custom_engineering,
        c.requires_special_test,
        c.requires_custom_paint,
        c.base_price_adjustment,
        tm.base_msrp + coalesce(c.base_price_adjustment, 0) as total_configuration_price,
        c.created_at
    from configurations c
    left join truck_models tm on c.truck_model_id = tm.truck_model_id
)

select * from final

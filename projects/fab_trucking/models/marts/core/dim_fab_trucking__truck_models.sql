{{ config(materialized='table') }}

with truck_models as (
    select * from {{ ref('stg_fab_trucking__truck_models') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['truck_model_id']) }} as truck_model_key,
        truck_model_id,
        tenant_id,
        model_code,
        model_name,
        model_year,
        segment,
        base_msrp,
        created_at,
        updated_at
    from truck_models
)

select * from final

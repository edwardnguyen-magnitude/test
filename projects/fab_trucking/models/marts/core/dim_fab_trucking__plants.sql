{{ config(materialized='table') }}

with plants as (
    select * from {{ ref('stg_fab_trucking__plants') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['plant_id']) }} as plant_key,
        plant_id,
        tenant_id,
        plant_code,
        plant_name,
        location_country,
        location_region,
        time_zone,
        capacity_trucks_per_day,
        manager_employee_id,
        plant_type,
        status,
        status = 'ACTIVE' as is_active,
        created_at,
        updated_at
    from plants
)

select * from final

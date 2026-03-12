{{ config(materialized='table') }}

with workcenters as (
    select * from {{ ref('stg_fab_trucking__workcenters') }}
),

plants as (
    select * from {{ ref('dim_fab_trucking__plants') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['w.workcenter_id']) }} as workcenter_key,
        w.workcenter_id,
        w.tenant_id,
        w.plant_id,
        p.plant_code,
        p.plant_name,
        w.workcenter_code,
        w.workcenter_name,
        w.area,
        w.capacity_units_per_shift,
        w.shift_pattern,
        w.status,
        w.status = 'ACTIVE' as is_active,
        w.created_at
    from workcenters w
    left join plants p on w.plant_id = p.plant_id
)

select * from final

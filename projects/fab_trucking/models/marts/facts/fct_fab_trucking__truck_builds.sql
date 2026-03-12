{{ config(materialized='table') }}

with trucks as (
    select * from {{ ref('stg_fab_trucking__trucks') }}
),

plants as (
    select * from {{ ref('dim_fab_trucking__plants') }}
),

configurations as (
    select * from {{ ref('dim_fab_trucking__configurations') }}
),

customers as (
    select * from {{ ref('dim_fab_trucking__customers') }}
),

final as (
    select
        -- Keys
        {{ dbt_utils.generate_surrogate_key(['t.truck_build_id']) }} as truck_build_key,
        t.truck_build_id,
        t.vin,
        t.tenant_id,

        -- Dimension keys
        p.plant_key,
        t.plant_id,
        cfg.configuration_key,
        t.configuration_id,
        t.truck_model_id,
        cust.customer_key,
        t.customer_id,

        -- Dates
        t.planned_build_date,
        t.actual_build_start_ts,
        t.actual_build_end_ts,
        t.planned_ship_date,
        t.planned_customer_delivery_date,

        -- Status
        t.current_status,
        t.build_source_system,

        -- Calculated metrics
        case
            when t.actual_build_end_ts is not null and t.actual_build_start_ts is not null
            then extract(epoch from (t.actual_build_end_ts - t.actual_build_start_ts)) / 3600
            else null
        end as build_duration_hours,

        case
            when t.actual_build_end_ts is not null
            then t.actual_build_end_ts::date - t.planned_build_date
            else null
        end as build_delay_days,

        -- Value
        cfg.total_configuration_price as truck_value,

        -- Flags
        t.current_status = 'BUILT_OFFLINE' as is_offline,
        t.current_status = 'DELIVERED' as is_delivered,
        t.current_status = 'SHIPPED' as is_shipped,

        -- Timestamps
        t.created_at,
        t.updated_at

    from trucks t
    left join plants p on t.plant_id = p.plant_id
    left join configurations cfg on t.configuration_id = cfg.configuration_id
    left join customers cust on t.customer_id = cust.customer_id
)

select * from final

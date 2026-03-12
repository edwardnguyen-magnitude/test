{{ config(materialized='view') }}

with offline_cases as (
    select * from {{ ref('stg_fab_trucking__offline_cases') }}
),

trucks as (
    select * from {{ ref('stg_fab_trucking__trucks') }}
),

plants as (
    select * from {{ ref('stg_fab_trucking__plants') }}
),

configurations as (
    select * from {{ ref('stg_fab_trucking__configurations') }}
),

truck_models as (
    select * from {{ ref('stg_fab_trucking__truck_models') }}
),

customers as (
    select * from {{ ref('stg_fab_trucking__customers') }}
),

enriched as (
    select
        -- Offline case identifiers
        oc.offline_case_id,
        oc.vin,
        oc.truck_build_id,
        oc.tenant_id,

        -- Plant info
        oc.plant_id,
        p.plant_code,
        p.plant_name,

        -- Truck info
        t.current_status as truck_status,
        t.planned_build_date,
        t.actual_build_start_ts,
        t.actual_build_end_ts,
        t.planned_ship_date,
        t.planned_customer_delivery_date,

        -- Configuration info
        t.configuration_id,
        cfg.configuration_code,
        cfg.description as configuration_description,
        cfg.has_custom_engineering,
        cfg.requires_special_test,
        cfg.requires_custom_paint,

        -- Truck model info
        t.truck_model_id,
        tm.model_code,
        tm.model_name,
        tm.segment,
        tm.base_msrp,
        cfg.base_price_adjustment,
        tm.base_msrp + coalesce(cfg.base_price_adjustment, 0) as total_truck_value,

        -- Customer info
        t.customer_id,
        cust.customer_code,
        cust.customer_name,
        cust.customer_segment,

        -- Offline case details
        oc.opened_ts,
        oc.closed_ts,
        oc.offline_reason_primary,
        oc.offline_reason_secondary,
        oc.current_offline_status,
        oc.responsible_function,
        oc.priority_score,
        oc.priority_band,
        oc.source_system,

        -- Calculated fields
        case
            when oc.closed_ts is not null
            then extract(epoch from (oc.closed_ts - oc.opened_ts)) / 86400
            else extract(epoch from (current_timestamp - oc.opened_ts)) / 86400
        end as offline_age_days,

        case
            when oc.closed_ts is null then true
            else false
        end as is_open,

        -- Ship delay calculation
        case
            when t.planned_ship_date < current_date and oc.closed_ts is null
            then current_date - t.planned_ship_date
            else 0
        end as ship_delay_days,

        -- Timestamps
        oc.created_at,
        oc.updated_at

    from offline_cases oc
    left join trucks t on oc.truck_build_id = t.truck_build_id
    left join plants p on oc.plant_id = p.plant_id
    left join configurations cfg on t.configuration_id = cfg.configuration_id
    left join truck_models tm on t.truck_model_id = tm.truck_model_id
    left join customers cust on t.customer_id = cust.customer_id
)

select * from enriched
